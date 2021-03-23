package postgres

import (
	"context"
	dbsql "database/sql"
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

	createTxn = "INSERT INTO relation_tuple_transaction DEFAULT VALUES RETURNING id"

	// This is the largest positive integer possible in postgresql
	liveDeletedTxnID = uint64(9223372036854775807)

	defaultWatchBufferLength = 128
)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevision = psql.Select("MAX(id)").From(tableTransaction)

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
) (datastore.Datastore, error) {
	if watchBufferLength == 0 {
		watchBufferLength = defaultWatchBufferLength
	}

	connectStr, err := pq.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := sqlx.Connect("postgres", connectStr)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &pgDatastore{db: db,
		watchBufferLength:        watchBufferLength,
		revisionFuzzingTimedelta: revisionFuzzingTimedelta,
	}, nil
}

type pgDatastore struct {
	db                       *sqlx.DB
	watchBufferLength        uint16
	revisionFuzzingTimedelta time.Duration
}

func (pgd *pgDatastore) SyncRevision(ctx context.Context) (uint64, error) {
	tx, err := pgd.db.Beginx()
	if err != nil {
		return 0, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback()

	return loadRevision(ctx, tx)
}

func (pgd *pgDatastore) Revision(ctx context.Context) (uint64, error) {
	tx, err := pgd.db.Beginx()
	if err != nil {
		return 0, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback()

	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var now time.Time
	err = tx.QueryRowContext(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	lowerBound := now.Add(-1 * pgd.revisionFuzzingTimedelta)
	sql, args, err := getMatchingRevision.Where(sq.GtOrEq{colTimestamp: lowerBound}).ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	rows, err := tx.QueryxContext(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var candidates []uint64
	for rows.Next() {
		var newCandidate uint64
		err := rows.Scan(&newCandidate)
		if err != nil {
			return 0, fmt.Errorf(errRevision, err)
		}

		candidates = append(candidates, newCandidate)
	}
	if rows.Err() != nil {
		return 0, fmt.Errorf(errRevision, rows.Err())
	}

	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))], nil
	} else {
		return loadRevision(ctx, tx)
	}
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

func createNewTransaction(tx *sqlx.Tx) (newTxnID uint64, err error) {
	err = tx.QueryRowx(createTxn).Scan(&newTxnID)
	return
}
