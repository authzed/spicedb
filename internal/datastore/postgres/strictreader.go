package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// strictReaderQueryFuncs wraps a DBFuncQuerier and adds a strict read assertion to all queries.
// This assertion ensures that the transaction is not reading from the future or from a
// transaction that has not been committed on the replica.
type strictReaderQueryFuncs struct {
	wrapped  pgxcommon.DBFuncQuerier
	revision postgresRevision
}

func (srqf strictReaderQueryFuncs) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, args ...any) error {
	// NOTE: it is *required* for the pgx.QueryExecModeSimpleProtocol to be added as pgx will otherwise wrap
	// the query as a prepared statement, which does *not* support running more than a single statement at a time.
	return srqf.rewriteError(srqf.wrapped.ExecFunc(ctx, tagFunc, srqf.addAssertToSelectSQL(sql), append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...))
}

func (srqf strictReaderQueryFuncs) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, args ...any) error {
	return srqf.rewriteError(srqf.wrapped.QueryFunc(ctx, rowsFunc, srqf.addAssertToSelectSQL(sql), append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...))
}

func (srqf strictReaderQueryFuncs) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, args ...any) error {
	return srqf.rewriteError(srqf.wrapped.QueryRowFunc(ctx, rowFunc, srqf.addAssertToSelectSQL(sql), append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...))
}

func (srqf strictReaderQueryFuncs) rewriteError(err error) error {
	if err == nil {
		return nil
	}

	if pgxcommon.IsReplicationLagError(err) {
		return common.NewRevisionUnavailableError(fmt.Errorf("revision %s is not available on the replica", srqf.revision.String()))
	}

	return err
}

func (srqf strictReaderQueryFuncs) addAssertToSelectSQL(sql string) string {
	spiceerrors.DebugAssertf(func() bool {
		return strings.HasPrefix(sql, "SELECT ")
	}, "strictReaderQueryFuncs can only wrap SELECT queries")

	// The guard checks that the replica's current snapshot contains every
	// transaction that is visible in the revision being read; i.e. that the
	// revision's data is fully present on this replica. This is the inline
	// equivalent of the revision comparison CheckRevision performs, evaluated on
	// the replica's own connection so it is valid even when a load balancer sits in
	// front of the read pool. When it does not hold, the read raises and
	// rewriteError maps the "replica missing revision" error to a
	// RevisionUnavailableError so the caller can fall back to the primary.
	//
	// The guard *must* raise from within the SELECT itself: it is the only
	// statement whose verdict is tied to the snapshot the rows are actually read
	// under. See replicaRevisionGuardExpression.
	//
	// The trailing DO block re-checks the same condition against the replica's
	// snapshot at that later point. It is a backstop, not the guard: it can only
	// turn a read that slipped through into an error, never the reverse, because
	// snapshots only advance and this condition is monotonic in that advance. We
	// run the query *first* as PGX will not be able to read rows if the assertion
	// is run first.
	wrapped := fmt.Sprintf(`
		SELECT * FROM (%s) AS results WHERE %s;
		DO $$
		BEGIN
			ASSERT (select %s), 'replica missing revision';
		END
		$$;
	`, sql,
		srqf.revision.snapshot.replicaRevisionGuardExpression(),
		srqf.revision.snapshot.replicaContainsRevisionPredicate())
	return wrapped
}

// replicaRevisionGuardExpression returns a SQL boolean expression, to be used as
// the WHERE clause of the wrapped SELECT, which is true when the replica's
// current snapshot contains every transaction visible in s (see
// replicaContainsRevisionPredicate) and otherwise *raises*.
//
// Raising, rather than filtering the rows away, is what makes the guard sound.
// Under READ COMMITTED every statement of a multi-statement query takes its own
// snapshot, so a filter-only guard paired with a trailing assertion has a race:
// if the replica catches up between the two statements, the filter drops every
// row (its snapshot was behind) while the assertion passes (its snapshot is not),
// and the caller sees zero rows and no error, i.e. reports the object as not
// existing. Raising from the reading statement ties the verdict to the snapshot
// the rows are read under.
//
// SQL expressions cannot raise directly, so the failure is expressed as a cast
// that cannot succeed, whose message embeds the marker IsReplicationLagError
// looks for. The cast target is boolean because its input function rejects
// invalid input on every supported Postgres version; xid8's does not (before
// Postgres 15 it silently parses unparseable text as 0, which would disable the
// guard entirely).
//
// The expression is deliberately built only from stable and parallel-safe
// functions: those properties are what make Postgres treat it as a gating
// "One-Time Filter", evaluated exactly once before the scan and regardless of
// how many rows match (a volatile expression is instead pushed into the scan's
// per-row filter, where it would never run for a query that matches nothing),
// and let the read still use parallel plans. Only the CASE's untaken branch
// raises, and a stable function keeps the planner from folding it into a
// plan-time error.
func (s pgSnapshot) replicaRevisionGuardExpression() string {
	return fmt.Sprintf(
		`CASE WHEN %s THEN true ELSE (`+
			`'replica missing revision (replica snapshot ' || pg_current_snapshot()::text || `+
			`' does not contain revision %s)')::boolean END`,
		s.replicaContainsRevisionPredicate(), s.String(),
	)
}

// replicaContainsRevisionPredicate returns a SQL boolean expression, to be
// evaluated on a replica's connection, that is true when the replica's current
// snapshot contains every transaction visible in s. It is the SQL form of "the
// replica's live snapshot dominates this revision":
//
//   - the replica's frontier (its snapshot xmax) must have reached the revision's
//     xmax, so no transaction the revision can see is still in the replica's
//     future; and
//   - the replica must not still consider in-progress any transaction below the
//     revision's xmax that the revision treats as committed (i.e. that is not in
//     the revision's own in-progress list). Commit order does not follow xid
//     order, so a lower-xid transaction the revision sees as committed can still be
//     replaying on the replica even once higher xids have been applied; such a
//     transaction would otherwise be silently missing.
func (s pgSnapshot) replicaContainsRevisionPredicate() string {
	revisionXip := make([]string, 0, len(s.xipList))
	for _, xip := range s.xipList {
		revisionXip = append(revisionXip, fmt.Sprintf("'%d'::xid8", xip))
	}

	return fmt.Sprintf(
		`pg_snapshot_xmax(pg_current_snapshot()) >= '%d'::xid8 `+
			`AND NOT EXISTS (`+
			`SELECT 1 FROM pg_snapshot_xip(pg_current_snapshot()) AS replica_xip `+
			`WHERE replica_xip < '%d'::xid8 AND replica_xip <> ALL (ARRAY[%s]::xid8[]))`,
		s.xmax, s.xmax, strings.Join(revisionXip, ", "),
	)
}
