package postgres

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/alecthomas/units"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/ngrok/sqlmw"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
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

	batchDeleteSize = 1000
)

var gcDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "postgres_gc_duration",
	Help:    "postgres garbage collection duration distribution in seconds.",
	Buckets: []float64{0.01, 0.1, 0.5, 1, 5, 10, 25, 60, 120},
})

func init() {
	dbsql.Register(tracingDriverName, sqlmw.Driver(stdlib.GetDefaultDriver(), new(traceInterceptor)))
	prometheus.MustRegister(gcDurationHistogram)
}

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevision = psql.Select("MAX(id)").From(tableTransaction)

	getRevisionRange = psql.Select("MIN(id)", "MAX(id)").From(tableTransaction)

	getNow = psql.Select("NOW()")

	tracer = otel.Tracer("spicedb/internal/datastore/postgres")
)

type sqlFilter interface {
	ToSql() (string, []interface{}, error)
}

// NewPostgresDatastore initializes a SpiceDB datastore that uses a PostgreSQL
// database by leveraging manual book-keeping to implement revisioning.
//
// This datastore is also tested to be compatible with CockroachDB.
func NewPostgresDatastore(
	url string,
	options ...Option,
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

	pgxConfig.ConnConfig.Logger = zerologadapter.NewLogger(log.Logger)

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

	datastore := &pgDatastore{
		dburl:                     url,
		dbpool:                    dbpool,
		watchBufferLength:         config.watchBufferLength,
		revisionFuzzingTimedelta:  config.revisionFuzzingTimedelta,
		gcWindowInverted:          -1 * config.gcWindow,
		gcInterval:                config.gcInterval,
		gcMaxOperationTime:        config.gcMaxOperationTime,
		splitAtEstimatedQuerySize: config.splitAtEstimatedQuerySize,
		disposed:                  make(chan struct{}),
	}

	// Start a goroutine for garbage collection.
	if datastore.gcInterval > 0*time.Minute {
		datastore.wg.Add(1)
		go datastore.runGarbageCollector()
	} else {
		log.Warn().Msg("garbage collection disabled in postgres driver")
	}

	return datastore, nil
}

type pgDatastore struct {
	dburl                     string
	dbpool                    *pgxpool.Pool
	watchBufferLength         uint16
	revisionFuzzingTimedelta  time.Duration
	gcWindowInverted          time.Duration
	gcInterval                time.Duration
	gcMaxOperationTime        time.Duration
	splitAtEstimatedQuerySize units.Base2Bytes

	disposed chan struct{}
	wg       sync.WaitGroup
}

func (pgd *pgDatastore) runGarbageCollector() {
	log.Info().Dur("interval", pgd.gcInterval).Msg("garbage collection worker started for postgres driver")

	defer pgd.wg.Done()
	for {
		select {
		case <-pgd.disposed:
			log.Info().Msg("shutting down postgres GC")
			return
		case <-time.After(pgd.gcInterval):
			err := pgd.collectGarbage()
			if err != nil {
				log.Warn().Err(err).Msg("error when attempting to perform garbage collection")
			} else {
				log.Debug().Msg("garbage collection completed for postgres")
			}
		}
	}
}

func (pgd *pgDatastore) getNow(ctx context.Context) (time.Time, error) {
	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Now(), err
	}

	var now time.Time
	err = pgd.dbpool.QueryRow(datastore.SeparateContextWithTracing(ctx), nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Now(), err
	}

	// RelationTupleTransaction is not timezone aware
	// Explicitly use UTC before using as a query arg
	now = now.UTC()

	return now, nil
}

func (pgd *pgDatastore) collectGarbage() error {
	startTime := time.Now()
	defer func() {
		gcDurationHistogram.Observe(float64(time.Since(startTime).Seconds()))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), pgd.gcMaxOperationTime)
	defer cancel()

	// Ensure the database is ready.
	ready, err := pgd.IsReady(ctx)
	if err != nil {
		return err
	}

	if !ready {
		log.Warn().Msg("cannot perform postgres garbage collection: postgres driver is not yet ready")
		return nil
	}

	now, err := pgd.getNow(ctx)
	if err != nil {
		return err
	}

	before := now.Add(pgd.gcWindowInverted)
	log.Debug().Time("before", before).Msg("running postgres garbage collection")
	_, _, err = pgd.collectGarbageBefore(ctx, before)
	return err
}

func (pgd *pgDatastore) collectGarbageBefore(ctx context.Context, before time.Time) (int64, int64, error) {
	// Find the highest transaction ID before the GC window.
	sql, args, err := getRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	value := pgtype.Int8{}
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&value)
	if err != nil {
		return 0, 0, err
	}

	if value.Status != pgtype.Present {
		log.Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return 0, 0, nil
	}

	var highest uint64
	err = value.AssignTo(&highest)
	if err != nil {
		return 0, 0, err
	}

	log.Trace().Uint64("highest_transaction_id", highest).Msg("retrieved transaction ID for GC")

	return pgd.collectGarbageForTransaction(ctx, highest)
}

func (pgd *pgDatastore) collectGarbageForTransaction(ctx context.Context, highest uint64) (int64, int64, error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	relCount, err := pgd.batchDelete(ctx, tableTuple, sq.LtOrEq{colDeletedTxn: highest})
	if err != nil {
		return 0, 0, err
	}

	log.Trace().Uint64("highest_transaction_id", highest).Int64("relationships_deleted", relCount).Msg("deleted stale relationships")

	// Delete all transaction rows with ID < the transaction ID. We don't delete the transaction
	// itself to ensure there is always at least one transaction present.
	transactionCount, err := pgd.batchDelete(ctx, tableTransaction, sq.Lt{colID: highest})
	if err != nil {
		return relCount, 0, err
	}

	log.Trace().Uint64("highest_transaction_id", highest).Int64("transactions_deleted", transactionCount).Msg("deleted stale transactions")
	return relCount, transactionCount, nil
}

func (pgd *pgDatastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	sql, args, err := psql.Select("id").From(tableName).Where(filter).ToSql()
	if err != nil {
		return -1, err
	}

	query := fmt.Sprintf(`WITH rows AS (%s)
		  DELETE FROM %s
		  WHERE id IN (SELECT id FROM rows);
	`, sql, tableName)

	var deletedCount int64
	for {
		cr, err := pgd.dbpool.Exec(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted := cr.RowsAffected()
		deletedCount += rowsDeleted
		if rowsDeleted < batchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}

func (pgd *pgDatastore) IsReady(ctx context.Context) (bool, error) {
	headMigration, err := migrations.DatabaseMigrations.HeadRevision()
	if err != nil {
		return false, fmt.Errorf("Invalid head migration found for postgres: %w", err)
	}

	currentRevision, err := migrations.NewAlembicPostgresDriver(pgd.dburl)
	if err != nil {
		return false, err
	}
	defer currentRevision.Dispose()

	version, err := currentRevision.Version()
	if err != nil {
		return false, err
	}

	return version == headMigration, nil
}

func (pgd *pgDatastore) Dispose() {
	// Close the channel to ensure the GC task is shut down.
	close(pgd.disposed)
	pgd.wg.Wait()
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
