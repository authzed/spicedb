package postgres

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"math/rand"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/alecthomas/units"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/ngrok/sqlmw"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
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
	dbsql.Register(tracingDriverName, sqlmw.Driver(stdlib.GetDefaultDriver(), new(traceInterceptor)))
}

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevision = psql.Select("MAX(id)").From(tableTransaction)

	getRevisionRange = psql.Select("MIN(id)", "MAX(id)").From(tableTransaction)

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
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// config must be initialized by ParseConfig
	pgxConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if config.maxOpenConns != nil {
		pgxConfig.MaxConns = int32(*config.maxOpenConns)
	}
	if config.minOpenConns != nil {
		pgxConfig.MinConns = int32(*config.minOpenConns)
	}
	if config.connMaxIdleTime != nil {
		pgxConfig.MaxConnIdleTime = *config.connMaxIdleTime
	}
	if config.connMaxLifetime != nil {
		pgxConfig.MaxConnLifetime = *config.connMaxLifetime
	}
	if config.healthCheckPeriod != nil {
		pgxConfig.HealthCheckPeriod = *config.healthCheckPeriod
	}

	dbpool, err := pgxpool.ConnectConfig(context.Background(), pgxConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if config.enablePrometheusStats {
		collector := NewPgxpoolStatsCollector(dbpool, "spicedb")
		err := prometheus.Register(collector)
		if err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
	}

	return &pgDatastore{
		dbpool:                    dbpool,
		watchBufferLength:         config.watchBufferLength,
		revisionFuzzingTimedelta:  config.revisionFuzzingTimedelta,
		gcWindowInverted:          -1 * config.gcWindow,
		splitAtEstimatedQuerySize: config.splitAtEstimatedQuerySize,
	}, nil
}

type pgDatastore struct {
	dbpool                    *pgxpool.Pool
	watchBufferLength         uint16
	revisionFuzzingTimedelta  time.Duration
	gcWindowInverted          time.Duration
	splitAtEstimatedQuerySize units.Base2Bytes
}

func (pgd *pgDatastore) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "SyncRevision")
	defer span.End()

	revision, err := pgd.loadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	return revisionFromTransaction(revision), nil
}

func (pgd *pgDatastore) Revision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "Revision")
	defer span.End()

	lower, upper, err := pgd.computeRevisionRange(ctx, -1*pgd.revisionFuzzingTimedelta)
	if err != nil && err != pgx.ErrNoRows {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	if err == pgx.ErrNoRows {
		revision, err := pgd.loadRevision(ctx)
		if err != nil {
			return datastore.NoRevision, err
		}

		return revisionFromTransaction(revision), nil
	}

	if upper-lower == 0 {
		return revisionFromTransaction(upper), nil
	}

	return revisionFromTransaction(uint64(rand.Intn(int(upper-lower))) + lower), nil
}

func (pgd *pgDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	revisionTx := transactionFromRevision(revision)

	lower, upper, err := pgd.computeRevisionRange(ctx, pgd.gcWindowInverted)
	if err == nil {
		if revisionTx < lower {
			return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
		} else if revisionTx > upper {
			return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
		}

		return nil
	}

	if err != pgx.ErrNoRows {
		return fmt.Errorf(errCheckRevision, err)
	}

	// There are no unexpired rows
	sql, args, err := getRevision.ToSql()
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	var highest uint64
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&highest)
	if err == pgx.ErrNoRows {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	if revisionTx < highest {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	} else if revisionTx > highest {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	return nil
}

func (pgd *pgDatastore) loadRevision(ctx context.Context) (uint64, error) {
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

	sql, args, err := getRevision.ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var revision uint64
	err = pgd.dbpool.QueryRow(datastore.SeparateContextWithTracing(ctx), sql, args...).Scan(&revision)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf(errRevision, err)
	}

	return revision, nil
}

func (pgd *pgDatastore) computeRevisionRange(ctx context.Context, windowInverted time.Duration) (uint64, uint64, error) {
	ctx, span := tracer.Start(ctx, "computeRevisionRange")
	defer span.End()

	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return 0, 0, err
	}

	var now time.Time
	err = pgd.dbpool.QueryRow(datastore.SeparateContextWithTracing(ctx), nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return 0, 0, err
	}
	// RelationTupleTransaction is not timezone aware
	// Explicitly use UTC before using as a query arg
	now = now.UTC()

	span.AddEvent("DB returned value for NOW()")

	lowerBound := now.Add(windowInverted)

	sql, args, err := getRevisionRange.Where(sq.GtOrEq{colTimestamp: lowerBound}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	var lower, upper dbsql.NullInt64
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&lower, &upper)
	if err != nil {
		return 0, 0, err
	}

	span.AddEvent("DB returned revision range")

	if !lower.Valid || !upper.Valid {
		return 0, 0, pgx.ErrNoRows
	}

	return uint64(lower.Int64), uint64(upper.Int64), nil
}

func createNewTransaction(ctx context.Context, tx pgx.Tx) (newTxnID uint64, err error) {
	ctx, span := tracer.Start(ctx, "computeNewTransaction")
	defer span.End()

	err = tx.QueryRow(ctx, createTxn).Scan(&newTxnID)
	return
}

func revisionFromTransaction(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}

func transactionFromRevision(revision datastore.Revision) uint64 {
	return uint64(revision.IntPart())
}
