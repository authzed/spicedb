package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	errRevision       = "unable to find revision: %w"
	errCheckRevision  = "unable to check revision: %w"
	errRevisionFormat = "invalid revision format: %w"

	//   %[1] Name of xid column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Quantization period (in nanoseconds)
	//   %[5] Name of snapshot column
	insertHeartBeatRevision = `
	INSERT INTO %[2]s (%[1]s, %[5]s)
	SELECT pg_current_xact_id(), pg_current_snapshot()
	WHERE NOT EXISTS (
		SELECT 1
		FROM %[2]s rtt
		WHERE rtt.%[3]s >= TO_TIMESTAMP(FLOOR((EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000)/ %[4]d) * %[4]d / 1000000000) AT TIME ZONE 'utc'
        LIMIT 1
	);`

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
	//   %[6] Follower read delay (in nanoseconds)
	querySelectRevision = `
	WITH selected AS (SELECT (
		(SELECT %[1]s FROM %[2]s WHERE %[3]s >= TO_TIMESTAMP(FLOOR((EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000 - %[6]d)/ %[4]d) * %[4]d / 1000000000) AT TIME ZONE 'utc' ORDER BY %[3]s ASC LIMIT 1)
	) as xid)
	SELECT selected.xid,
	COALESCE((SELECT %[5]s FROM %[2]s WHERE %[1]s = selected.xid), (SELECT pg_current_snapshot())),
	%[4]d - CAST(EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000 as bigint) %% %[4]d
	FROM selected;`

	// queryValidTransaction will return a single row with three values:
	//   1) the transaction ID of the minimum valid (i.e. within the GC window) transaction
	//   2) the snapshot associated with the minimum valid transaction
	//   3) the current snapshot that would be used if a new transaction were created now
	//
	// The input values for the format string are:
	//   %[1] Name of xid column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Inverse of GC window (in seconds)
	//   %[5] Name of the snapshot column
	queryValidTransaction = `
	WITH minvalid AS (
		SELECT %[1]s, %[5]s
        FROM %[2]s
        WHERE 
            %[3]s >= NOW() - INTERVAL '%[4]f seconds'
          OR
             %[3]s = (SELECT MAX(%[3]s) FROM %[2]s)
        ORDER BY %[3]s ASC
        LIMIT 1
	)
	SELECT minvalid.%[1]s, minvalid.%[5]s, pg_current_snapshot() FROM minvalid;`

	queryCurrentSnapshot = `SELECT pg_current_snapshot();`

	queryCurrentTransactionID = `SELECT pg_current_xact_id()::text::integer;`
	queryLatestXID            = `SELECT max(xid)::text::integer FROM relation_tuple_transaction;`
)

func (pgd *pgDatastore) optimizedRevisionFunc(ctx context.Context) (datastore.Revision, time.Duration, error) {
	var revision xid8
	var snapshot pgSnapshot
	var validForNanos time.Duration
	if err := pgd.readPool.QueryRow(ctx, pgd.optimizedRevisionQuery).
		Scan(&revision, &snapshot, &validForNanos); err != nil {
		return datastore.NoRevision, 0, fmt.Errorf(errRevision, err)
	}

	snapshot = snapshot.markComplete(revision.Uint64)

	return postgresRevision{snapshot: snapshot, optionalTxID: revision}, validForNanos, nil
}

func (pgd *pgDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "HeadRevision")
	defer span.End()

	result, err := pgd.getHeadRevision(ctx, pgd.readPool)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return datastore.NoRevision, nil
	}

	return *result, nil
}

func (pgd *pgDatastore) getHeadRevision(ctx context.Context, querier common.Querier) (*postgresRevision, error) {
	var snapshot pgSnapshot
	if err := querier.QueryRow(ctx, queryCurrentSnapshot).Scan(&snapshot); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf(errRevision, err)
	}

	return &postgresRevision{snapshot: snapshot}, nil
}

func (pgd *pgDatastore) CheckRevision(ctx context.Context, revisionRaw datastore.Revision) error {
	revision, ok := revisionRaw.(postgresRevision)
	if !ok {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.CouldNotDetermineRevision)
	}

	var minXid xid8
	var minSnapshot, currentSnapshot pgSnapshot
	if err := pgd.readPool.QueryRow(ctx, pgd.validTransactionQuery).
		Scan(&minXid, &minSnapshot, &currentSnapshot); err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	if revisionRaw.GreaterThan(postgresRevision{snapshot: currentSnapshot}) {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}
	if minSnapshot.markComplete(minXid.Uint64).GreaterThan(revision.snapshot) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

// RevisionFromString reverses the encoding process performed by MarshalBinary and String.
func (pgd *pgDatastore) RevisionFromString(revisionStr string) (datastore.Revision, error) {
	return ParseRevisionString(revisionStr)
}

// ParseRevisionString parses a revision string into a Postgres revision.
func ParseRevisionString(revisionStr string) (rev datastore.Revision, err error) {
	rev, err = parseRevisionProto(revisionStr)
	if err != nil {
		decimalRev, decimalErr := parseRevisionDecimal(revisionStr)
		if decimalErr != nil {
			// If decimal ALSO had an error than it was likely just a mangled original input
			return
		}
		return decimalRev, nil
	}
	return
}

func parseRevisionProto(revisionStr string) (datastore.Revision, error) {
	protoBytes, err := base64.StdEncoding.DecodeString(revisionStr)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevisionFormat, err)
	}

	decoded := implv1.PostgresRevision{}
	if err := decoded.UnmarshalVT(protoBytes); err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevisionFormat, err)
	}

	xminInt, err := safecast.ToInt64(decoded.Xmin)
	if err != nil {
		return datastore.NoRevision, spiceerrors.MustBugf("could not cast xmin to int64")
	}

	var xips []uint64
	if len(decoded.RelativeXips) > 0 {
		xips = make([]uint64, len(decoded.RelativeXips))
		for i, relativeXip := range decoded.RelativeXips {
			xip := xminInt + relativeXip
			uintXip, err := safecast.ToUint64(xip)
			if err != nil {
				return datastore.NoRevision, spiceerrors.MustBugf("could not cast xip to int64")
			}
			xips[i] = uintXip
		}
	}

	xmax, err := safecast.ToUint64(xminInt + decoded.RelativeXmax)
	if err != nil {
		return datastore.NoRevision, spiceerrors.MustBugf("could not cast xmax to int64")
	}

	return postgresRevision{
		snapshot: pgSnapshot{
			xmin:    decoded.Xmin,
			xmax:    xmax,
			xipList: xips,
		},
		optionalTxID:           xid8{Uint64: decoded.OptionalTxid, Valid: decoded.OptionalTxid != 0},
		optionalNanosTimestamp: decoded.OptionalTimestamp,
	}, nil
}

// MaxLegacyXIPDelta is the maximum allowed delta between the xmin and
// xmax revisions IDs on a *legacy* revision stored as a revision decimal.
// This is set to prevent a delta that is too large from blowing out the
// memory usage of the allocated slice, or even causing a panic in the case
// of a VERY large delta (which can be produced by, for example, a CRDB revision
// being given to a Postgres datastore accidentally).
const MaxLegacyXIPDelta = 1000

// parseRevisionDecimal parses a deprecated decimal.Decimal encoding of the revision
// with an optional xmin component, in the format of revision.xmin, e.g. 100.99.
// Because we're encoding to a snapshot, we want the revision to be considered visible,
// so we set the xmax and xmin for 1 past the encoded revision for the simple cases.
func parseRevisionDecimal(revisionStr string) (datastore.Revision, error) {
	components := strings.Split(revisionStr, ".")
	numComponents := len(components)
	if numComponents != 1 && numComponents != 2 {
		return datastore.NoRevision, fmt.Errorf(
			errRevisionFormat,
			fmt.Errorf("wrong number of components %d != 1 or 2", len(components)),
		)
	}

	xid, err := strconv.ParseUint(components[0], 10, 64)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevisionFormat, err)
	}

	xmax := xid + 1
	xmin := xid + 1

	if numComponents == 2 {
		xminCandidate, err := strconv.ParseUint(components[1], 10, 64)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errRevisionFormat, err)
		}
		if xminCandidate < xid {
			xmin = xminCandidate
		}
	}

	var xipList []uint64
	if xmax > xmin {
		// Ensure that the delta is not too large to cause memory issues or a panic.
		if xmax-xmin > MaxLegacyXIPDelta {
			return nil, fmt.Errorf("received revision delta in excess of that expected; are you sure you're not passing a ZedToken from an incompatible datastore?")
		}

		// TODO(jschorr): Remove this deprecated code path once we have per-datastore-marked ZedTokens.
		xipList = make([]uint64, 0, xmax-xmin)
		for i := xmin; i < xid; i++ {
			xipList = append(xipList, i)
		}
	}

	return postgresRevision{snapshot: pgSnapshot{
		xmin:    xmin,
		xmax:    xmax,
		xipList: xipList,
	}}, nil
}

var emptyMetadata = map[string]any{}

func createNewTransaction(ctx context.Context, tx pgx.Tx, metadata map[string]any) (newXID xid8, newSnapshot pgSnapshot, err error) {
	ctx, span := tracer.Start(ctx, "createNewTransaction")
	defer span.End()

	if metadata == nil {
		metadata = emptyMetadata
	}

	sql, args, err := createTxn.Values(metadata).Suffix("RETURNING " + schema.ColXID + ", " + schema.ColSnapshot).ToSql()
	if err != nil {
		return
	}

	cterr := tx.QueryRow(ctx, sql, args...).Scan(&newXID, &newSnapshot)
	if cterr != nil {
		err = fmt.Errorf("error when trying to create a new transaction: %w", cterr)
	}
	return
}

type postgresRevision struct {
	snapshot               pgSnapshot
	optionalTxID           xid8
	optionalNanosTimestamp uint64
	optionalMetadata       map[string]any
}

func (pr postgresRevision) ByteSortable() bool {
	return false
}

func (pr postgresRevision) Equal(rhsRaw datastore.Revision) bool {
	rhs, ok := rhsRaw.(postgresRevision)
	return ok && pr.snapshot.Equal(rhs.snapshot)
}

func (pr postgresRevision) GreaterThan(rhsRaw datastore.Revision) bool {
	if rhsRaw == datastore.NoRevision {
		return true
	}

	rhs, ok := rhsRaw.(postgresRevision)
	return ok && pr.snapshot.GreaterThan(rhs.snapshot)
}

func (pr postgresRevision) LessThan(rhsRaw datastore.Revision) bool {
	rhs, ok := rhsRaw.(postgresRevision)
	return ok && pr.snapshot.LessThan(rhs.snapshot)
}

func (pr postgresRevision) DebugString() string {
	return pr.snapshot.String()
}

func (pr postgresRevision) String() string {
	return base64.StdEncoding.EncodeToString(pr.mustMarshalBinary())
}

func (pr postgresRevision) mustMarshalBinary() []byte {
	serialized, err := pr.MarshalBinary()
	if err != nil {
		panic(fmt.Sprintf("unexpected error marshaling proto: %s", err))
	}
	return serialized
}

// OptionalTransactionID returns the transaction ID at which this revision happened. This value is optionally
// loaded from the database and may not be present.
func (pr postgresRevision) OptionalTransactionID() (xid8, bool) {
	if !pr.optionalTxID.Valid {
		return xid8{}, false
	}

	return pr.optionalTxID, true
}

// OptionalNanosTimestamp returns a unix epoch timestamp in nanos representing the time at which the transaction committed
// as defined by the Postgres primary. This is not guaranteed to be monotonically increasing
func (pr postgresRevision) OptionalNanosTimestamp() (uint64, bool) {
	if pr.optionalNanosTimestamp == 0 {
		return 0, false
	}

	return pr.optionalNanosTimestamp, true
}

// MarshalBinary creates a version of the snapshot that uses relative encoding
// for xmax and xip list values to save bytes when encoded as varint protos.
// For example, snapshot 1001:1004:1001,1003 becomes 1000:3:0,2.
func (pr postgresRevision) MarshalBinary() ([]byte, error) {
	xminInt, err := safecast.ToInt64(pr.snapshot.xmin)
	if err != nil {
		return nil, spiceerrors.MustBugf("could not safely cast snapshot xip to int64: %v", err)
	}
	relativeXips := make([]int64, len(pr.snapshot.xipList))
	for i, xip := range pr.snapshot.xipList {
		intXip, err := safecast.ToInt64(xip)
		if err != nil {
			return nil, spiceerrors.MustBugf("could not safely cast snapshot xip to int64: %v", err)
		}
		relativeXips[i] = intXip - xminInt
	}

	relativeXmax, err := safecast.ToInt64(pr.snapshot.xmax)
	if err != nil {
		return nil, spiceerrors.MustBugf("could not safely cast snapshot xmax to int64: %v", err)
	}
	protoRevision := implv1.PostgresRevision{
		Xmin:         pr.snapshot.xmin,
		RelativeXmax: relativeXmax - xminInt,
		RelativeXips: relativeXips,
	}

	return protoRevision.MarshalVT()
}

var _ datastore.Revision = postgresRevision{}

func revisionKeyFunc(rev postgresRevision) uint64 {
	return rev.optionalTxID.Uint64
}
