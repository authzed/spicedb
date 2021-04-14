package postgres

import (
	"context"
	"database/sql"
	dbsql "database/sql"
	"fmt"
	"math/rand"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/ngrok/sqlmw"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"

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

	tracingDriverName = "postgres-tracing"
)

func init() {
	sql.Register(tracingDriverName, sqlmw.Driver(pq.Driver{}, new(traceInterceptor)))
}

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevision = psql.Select("MAX(id)").From(tableTransaction)

	getRevisionRange = psql.Select("MIN(id)", "MAX(id)").From(tableTransaction)

	getMatchingRevision = psql.Select(colID).From(tableTransaction)

	getNow = psql.Select("NOW()")

	tracer = otel.Tracer("spicedb/internal/datastore/postgres")
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
	options ...PostgresOption,
) (datastore.Datastore, error) {
	connectStr, err := pq.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := sqlx.Connect(config.driver, connectStr)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if config.enablePrometheusStats {
		collector := sqlstats.NewStatsCollector("spicedb", db)
		err := prometheus.Register(collector)
		if err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
	}

	if config.maxOpenConns != nil {
		db.SetMaxOpenConns(*config.maxOpenConns)
	}
	if config.maxIdleConns != nil {
		db.SetMaxIdleConns(*config.maxIdleConns)
	}
	if config.connMaxIdleTime != nil {
		db.SetConnMaxIdleTime(*config.connMaxIdleTime)
	}
	if config.connMaxLifetime != nil {
		db.SetConnMaxLifetime(*config.connMaxLifetime)
	}

	return &pgDatastore{
		db:                       db,
		watchBufferLength:        config.watchBufferLength,
		revisionFuzzingTimedelta: config.revisionFuzzingTimedelta,
		gcWindowInverted:         -1 * config.gcWindow,
	}, nil
}

type pgDatastore struct {
	db                       *sqlx.DB
	watchBufferLength        uint16
	revisionFuzzingTimedelta time.Duration
	gcWindowInverted         time.Duration
}

func (pgd *pgDatastore) beginDBTransaction(ctx context.Context) (*sqlx.Tx, error) {
	_, span := tracer.Start(ctx, "beginDBTransaction")
	defer span.End()
	return pgd.db.BeginTxx(ctx, nil)
}

func (pgd *pgDatastore) SyncRevision(ctx context.Context) (uint64, error) {
	ctx, span := tracer.Start(ctx, "SyncRevision")
	defer span.End()

	tx, err := pgd.beginDBTransaction(ctx)
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}
	defer tx.Rollback()

	return loadRevision(ctx, tx)
}

func (pgd *pgDatastore) Revision(ctx context.Context) (uint64, error) {
	ctx, span := tracer.Start(ctx, "Revision")
	defer span.End()

	tx, err := pgd.beginDBTransaction(ctx)
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
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	tx, err := pgd.beginDBTransaction(ctx)
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
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

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
	ctx, span := tracer.Start(ctx, "computeRevisionRange")
	defer span.End()

	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return 0, 0, err
	}

	var now time.Time
	err = tx.QueryRowContext(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return 0, 0, err
	}

	span.AddEvent("DB returned value for NOW()")

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

	span.AddEvent("DB returned revision range")

	if !lower.Valid || !upper.Valid {
		return 0, 0, dbsql.ErrNoRows
	}

	return uint64(lower.Int64), uint64(upper.Int64), nil
}

func createNewTransaction(ctx context.Context, tx *sqlx.Tx) (newTxnID uint64, err error) {
	ctx, span := tracer.Start(ctx, "computeNewTransaction")
	defer span.End()

	err = tx.QueryRowxContext(ctx, createTxn).Scan(&newTxnID)
	return
}
