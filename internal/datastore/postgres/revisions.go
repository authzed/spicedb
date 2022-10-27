package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	errRevision       = "unable to find revision: %w"
	errCheckRevision  = "unable to check revision: %w"
	errRevisionFormat = "invalid revision format: %w"

	// querySelectRevision will round the database's timestamp down to the nearest
	// quantization period, and then find the first transaction (and its active xmin)
	// after that. If there are no transactions newer than the quantization period,
	// it just picks the latest transaction. It will also return the amount of
	// nanoseconds until the next optimized revision would be selected server-side,
	// for use with caching.
	//
	//   %[1] Name of xid column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Quantization period (in nanoseconds)
	//   %[5] Name of snapshot column
	querySelectRevision = `
	
	WITH selected AS (SELECT COALESCE(
		(SELECT %[1]s FROM %[2]s WHERE %[3]s >= TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000 / %[4]d) * %[4]d / 1000000000) AT TIME ZONE 'utc' ORDER BY %[3]s ASC LIMIT 1),
		(SELECT %[1]s FROM %[2]s ORDER BY %[3]s DESC LIMIT 1)
	) as xid)
	SELECT selected.xid,
	pg_snapshot_xmin(%[5]s),
	%[4]d - CAST(EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000 as bigint) %% %[4]d
	FROM selected INNER JOIN %[2]s ON selected.xid = %[2]s.%[1]s;`

	// queryValidTransaction will return a single row with two values, one boolean
	// for whether the specified transaction ID is newer than the garbage collection
	// window, and one boolean for whether the transaction ID represents a transaction
	// that will occur in the future.
	//
	//   %[1] Name of xid column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Inverse of GC window (in seconds)
	queryValidTransaction = `
	SELECT $1 >= (
		SELECT %[1]s FROM %[2]s WHERE %[3]s >= NOW() - INTERVAL '%[4]f seconds' ORDER BY %[3]s ASC LIMIT 1
	) as fresh, $1 > (
		SELECT %[1]s FROM %[2]s ORDER BY %[3]s DESC LIMIT 1
	) as unknown;`
)

func (pgd *pgDatastore) optimizedRevisionFunc(ctx context.Context) (datastore.Revision, time.Duration, error) {
	var revision, xmin xid8
	var validForNanos time.Duration
	if err := pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), pgd.optimizedRevisionQuery,
	).Scan(&revision, &xmin, &validForNanos); err != nil {
		return datastore.NoRevision, 0, fmt.Errorf(errRevision, err)
	}

	return postgresRevision{revision, xmin}, validForNanos, nil
}

func (pgd *pgDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	revision, xmin, err := pgd.loadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	return postgresRevision{revision, xmin}, nil
}

func (pgd *pgDatastore) CheckRevision(ctx context.Context, revisionRaw datastore.Revision) error {
	revision, ok := revisionRaw.(postgresRevision)
	if !ok {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.CouldNotDetermineRevision)
	}

	var freshEnough, unknown bool
	if err := pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), pgd.validTransactionQuery, revision.tx,
	).Scan(&freshEnough, &unknown); err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	if unknown {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}
	if !freshEnough {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

func (pgd *pgDatastore) RevisionFromString(revisionStr string) (datastore.Revision, error) {
	return parseRevision(revisionStr)
}

func parseRevision(revisionStr string) (datastore.Revision, error) {
	components := strings.Split(revisionStr, ".")
	numComponents := len(components)
	if numComponents != 1 && numComponents != 2 {
		return datastore.NoRevision, fmt.Errorf(
			errRevisionFormat,
			fmt.Errorf("wrong number of components %d != 1 or 2", len(components)),
		)
	}

	xid, err := strconv.ParseInt(components[0], 10, 64)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevisionFormat, err)
	}
	if xid < 0 {
		return datastore.NoRevision, fmt.Errorf(
			errRevisionFormat,
			errors.New("xid component is negative"),
		)
	}

	xmin := noXmin

	if numComponents == 2 {
		xminInt, err := strconv.ParseInt(components[1], 10, 64)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errRevisionFormat, err)
		}
		if xminInt < 0 {
			return datastore.NoRevision, fmt.Errorf(
				errRevisionFormat,
				errors.New("xmin component is negative"),
			)
		}
		xmin = xid8{
			Uint:   uint64(xminInt),
			Status: pgtype.Present,
		}
	}

	return postgresRevision{xid8{Uint: uint64(xid), Status: pgtype.Present}, xmin}, nil
}

func (pgd *pgDatastore) loadRevision(ctx context.Context) (xid8, xid8, error) {
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

	sql, args, err := getRevision.ToSql()
	if err != nil {
		return xid8{}, xid8{}, fmt.Errorf(errRevision, err)
	}

	var revision, xmin xid8
	err = pgd.dbpool.QueryRow(datastore.SeparateContextWithTracing(ctx), sql, args...).Scan(&revision, &xmin)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return xid8{}, xid8{}, nil
		}
		return xid8{}, xid8{}, fmt.Errorf(errRevision, err)
	}

	return revision, xmin, nil
}

func createNewTransaction(ctx context.Context, tx pgx.Tx) (newXID, newXmin xid8, err error) {
	ctx, span := tracer.Start(ctx, "createNewTransaction")
	defer span.End()

	err = tx.QueryRow(ctx, createTxn).Scan(&newXID, &newXmin)
	return
}

type postgresRevision struct {
	tx   xid8
	xmin xid8
}

var noXmin = xid8{
	Uint:   0,
	Status: pgtype.Undefined,
}

func (pr postgresRevision) Equal(rhsRaw datastore.Revision) bool {
	rhs, ok := validateRHS(rhsRaw)
	return ok && pr.tx.Uint == rhs.tx.Uint
}

func (pr postgresRevision) GreaterThan(rhsRaw datastore.Revision) bool {
	if rhsRaw == datastore.NoRevision {
		return true
	}

	rhs, ok := validateRHS(rhsRaw)
	return ok && pr.tx.Uint > rhs.tx.Uint &&
		((pr.xmin.Status == pgtype.Present && pr.xmin.Uint > rhs.tx.Uint) ||
			pr.xmin.Status != pgtype.Present)
}

func (pr postgresRevision) LessThan(rhsRaw datastore.Revision) bool {
	rhs, ok := validateRHS(rhsRaw)
	return ok && pr.tx.Uint < rhs.tx.Uint &&
		((rhs.xmin.Status == pgtype.Present && rhs.xmin.Uint > pr.tx.Uint) ||
			rhs.xmin.Status != pgtype.Present)
}

func (pr postgresRevision) String() string {
	if pr.xmin.Status == pgtype.Present {
		return fmt.Sprintf("%d.%d", pr.tx.Uint, pr.xmin.Uint)
	}
	return fmt.Sprintf("%d", pr.tx.Uint)
}

func (pr postgresRevision) MarshalBinary() ([]byte, error) {
	// We use the decimal library for this to keep it backward compatible with the old version.
	return decimal.NewFromInt(int64(pr.tx.Uint)).MarshalBinary()
}

func validateRHS(rhsRaw datastore.Revision) (postgresRevision, bool) {
	rhs, ok := rhsRaw.(postgresRevision)
	return rhs, ok && rhs.tx.Status == pgtype.Present
}

var _ datastore.Revision = postgresRevision{}
