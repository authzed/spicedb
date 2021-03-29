package postgres

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	tableNamespace   = "namespace_config"
	tableTransaction = "relation_tuple_transaction"
	tableTuple       = "relation_tuple"

	colID               = "id"
	colTimestamp        = "timestamp"
	colNamespace        = "namespace"
	colConfig           = "serialized_config"
	colCreatedTxn       = "created_transaction"
	colDeletedTxn       = "deleted_transaction"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"

	errUnableToInstantiate = "unable to instantiate datastore: %w"
	errRevision            = "unable to find revision: %w"
	errCheckRevision       = "unable to check revision: %w"

	createTxn = "INSERT INTO relation_tuple_transaction DEFAULT VALUES RETURNING id"

	// This is the largest positive integer possible in postgresql
	liveDeletedTxnID = uint64(9223372036854775807)

	defaultWatchBufferLength = 128
)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevision = psql.Select("MAX(id)").From(tableTransaction)

	getRevisionRange = psql.Select("MIN(id)", "MAX(id)").From(tableTransaction)

	getMatchingRevision = psql.Select(colID).From(tableTransaction)

	getNow = psql.Select("NOW()")
)

type RelationTupleRow struct {
	Namespace        string
	ObjectID         string
	Relation         string
	UsersetNamespace string
	UsersetObjectID  string
	UsersetRelation  string
}

func NewPostgresDatastore(
	url string,
	watchBufferLength uint16,
	revisionFuzzingTimedelta time.Duration,
	gcWindow time.Duration,
) (datastore.Datastore, error) {
	if watchBufferLength == 0 {
		watchBufferLength = defaultWatchBufferLength
	}

	if revisionFuzzingTimedelta > gcWindow {
		return nil, fmt.Errorf(
			errUnableToInstantiate,
			errors.New("gc window must be large than fuzzing window"),
		)
	}

	connectStr, err := pq.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := sqlx.Connect("postgres", connectStr)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &pgDatastore{
		db:                       db,
		watchBufferLength:        watchBufferLength,
		revisionFuzzingTimedelta: revisionFuzzingTimedelta,
		gcWindowInverted:         -1 * gcWindow,
	}, nil
}

type pgDatastore struct {
	db                       *sqlx.DB
	watchBufferLength        uint16
	revisionFuzzingTimedelta time.Duration
	gcWindowInverted         time.Duration
}

func (pgd *pgDatastore) SyncRevision(ctx context.Context) (uint64, error) {
	tx, err := pgd.db.Beginx()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}
	defer tx.Rollback()

	return loadRevision(ctx, tx)
}

func (pgd *pgDatastore) Revision(ctx context.Context) (uint64, error) {
	tx, err := pgd.db.Beginx()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}
	defer tx.Rollback()

	lower, upper, err := computeRevisionRange(ctx, tx, -1*pgd.revisionFuzzingTimedelta)
	if err != nil && err != dbsql.ErrNoRows {
		return 0, fmt.Errorf(errRevision, err)
	}

	if err == dbsql.ErrNoRows {
		return loadRevision(ctx, tx)
	}

	if upper-lower == 0 {
		return upper, nil
	}

	return uint64(rand.Intn(int(upper-lower))) + lower, nil
}

func (pgd *pgDatastore) CheckRevision(ctx context.Context, revision uint64) error {
	tx, err := pgd.db.Beginx()
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}
	defer tx.Rollback()

	lower, upper, err := computeRevisionRange(ctx, tx, pgd.gcWindowInverted)
	if err == nil {
		if revision >= lower && revision <= upper {
			return nil
		} else {
			return datastore.ErrInvalidRevision
		}
	}

	if err != dbsql.ErrNoRows {
		return fmt.Errorf(errCheckRevision, err)
	}

	// There are no unexpired rows
	sql, args, err := getRevision.ToSql()
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	var highest uint64
	err = tx.QueryRowxContext(ctx, sql, args...).Scan(&highest)
	if err == dbsql.ErrNoRows {
		return datastore.ErrInvalidRevision
	}
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	if revision != highest {
		return datastore.ErrInvalidRevision
	}

	return nil
}

func loadRevision(ctx context.Context, tx *sqlx.Tx) (uint64, error) {
	sql, args, err := getRevision.ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var revision uint64
	err = tx.QueryRowxContext(ctx, sql, args...).Scan(&revision)
	if err != nil {
		if err == dbsql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf(errRevision, err)
	}

	return revision, nil
}

func computeRevisionRange(ctx context.Context, tx *sqlx.Tx, windowInverted time.Duration) (uint64, uint64, error) {
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return 0, 0, err
	}

	var now time.Time
	err = tx.QueryRowContext(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return 0, 0, err
	}

	lowerBound := now.Add(windowInverted)

	sql, args, err := getRevisionRange.Where(sq.GtOrEq{colTimestamp: lowerBound}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	var lower, upper dbsql.NullInt64
	err = tx.QueryRowxContext(ctx, sql, args...).Scan(&lower, &upper)
	if err != nil {
		return 0, 0, err
	}

	if !lower.Valid || !upper.Valid {
		return 0, 0, dbsql.ErrNoRows
	}

	return uint64(lower.Int64), uint64(upper.Int64), nil
}

func createNewTransaction(tx *sqlx.Tx) (newTxnID uint64, err error) {
	err = tx.QueryRowx(createTxn).Scan(&newTxnID)
	return
}
