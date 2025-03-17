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
	return srqf.rewriteError(srqf.wrapped.ExecFunc(ctx, tagFunc, srqf.addAssertToSelectSQL(sql), append([]interface{}{pgx.QueryExecModeSimpleProtocol}, args...)...))
}

func (srqf strictReaderQueryFuncs) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, args ...any) error {
	return srqf.rewriteError(srqf.wrapped.QueryFunc(ctx, rowsFunc, srqf.addAssertToSelectSQL(sql), append([]interface{}{pgx.QueryExecModeSimpleProtocol}, args...)...))
}

func (srqf strictReaderQueryFuncs) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, args ...any) error {
	return srqf.rewriteError(srqf.wrapped.QueryRowFunc(ctx, rowFunc, srqf.addAssertToSelectSQL(sql), append([]interface{}{pgx.QueryExecModeSimpleProtocol}, args...)...))
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
	spiceerrors.DebugAssert(func() bool {
		return strings.HasPrefix(sql, "SELECT ")
	}, "strictReaderQueryFuncs can only wrap SELECT queries")

	// The assertion checks that the transaction is not reading from the future or from a
	// transaction that is still in-progress on the replica. If the transaction is not yet
	// available on the replica at all, the call to `pg_xact_status` will fail with an invalid
	// argument error and a message indicating that the xid "is in the future". If the transaction
	// does exist, but has not yet been committed (or aborted), the call to `pg_xact_status` will return
	// "in progress". rewriteError will catch these cases and return a RevisionUnavailableError.
	//
	// We run the query *first* (but filtered) as PGX will not be able to read rows if the assertion
	// is run first. However, we do not want to return any rows if the assertion will fail, so we add it
	// as a filter to the select as well.
	wrapped := fmt.Sprintf(`
		SELECT * FROM (%s) AS results WHERE pg_xact_status(%d::text::xid8) != 'in progress';
		DO $$
		BEGIN
			ASSERT (select pg_xact_status(%d::text::xid8) != 'in progress'), 'replica missing revision';
		END
		$$;
	`, sql, srqf.revision.snapshot.xmin-1, srqf.revision.snapshot.xmin-1)
	return wrapped
}
