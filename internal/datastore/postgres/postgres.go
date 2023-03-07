package postgres

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/pgxpoolprometheus"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/ngrok/sqlmw"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine           = "postgres"
	tableNamespace   = "namespace_config"
	tableTransaction = "relation_tuple_transaction"
	tableTuple       = "relation_tuple"
	tableCaveat      = "caveat"

	colXID               = "xid"
	colTimestamp         = "timestamp"
	colNamespace         = "namespace"
	colConfig            = "serialized_config"
	colCreatedXid        = "created_xid"
	colDeletedXid        = "deleted_xid"
	colSnapshot          = "snapshot"
	colObjectID          = "object_id"
	colRelation          = "relation"
	colUsersetNamespace  = "userset_namespace"
	colUsersetObjectID   = "userset_object_id"
	colUsersetRelation   = "userset_relation"
	colCaveatName        = "name"
	colCaveatDefinition  = "definition"
	colCaveatContextName = "caveat_name"
	colCaveatContext     = "caveat_context"

	errUnableToInstantiate = "unable to instantiate datastore: %w"

	// The parameters to this format string are:
	// 1: the created_xid or deleted_xid column name
	//
	// The placeholders are the snapshot and the expected boolean value respectively.
	snapshotAlive = "pg_visible_in_snapshot(%[1]s, ?) = ?"

	// This is the largest positive integer possible in postgresql
	liveDeletedTxnID = uint64(9223372036854775807)

	tracingDriverName = "postgres-tracing"

	batchDeleteSize = 1000

	pgSerializationFailure      = "40001"
	pgUniqueConstraintViolation = "23505"

	livingTupleConstraint = "uq_relation_tuple_living_xid"
)

func init() {
	dbsql.Register(tracingDriverName, sqlmw.Driver(stdlib.GetDefaultDriver(), new(traceInterceptor)))
}

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevision = psql.
			Select(colXID, colSnapshot).
			From(tableTransaction).
			OrderByClause(fmt.Sprintf("%s DESC", colXID)).
			Limit(1)

	createTxn = fmt.Sprintf(
		"INSERT INTO %s DEFAULT VALUES RETURNING %s, %s",
		tableTransaction,
		colXID,
		colSnapshot,
	)

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
	ds, err := newPostgresDatastore(url, options...)
	if err != nil {
		return nil, err
	}

	return proxy.NewSeparatingContextDatastoreProxy(ds), nil
}

func newPostgresDatastore(
	url string,
	options ...Option,
) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if config.migrationPhase != "" {
		log.Info().
			Str("phase", config.migrationPhase).
			Msg("postgres configured to use intermediate migration phase")
	}

	// config must be initialized by ParseConfig
	pgxConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	configurePool(config, pgxConfig)

	initializationContext, cancelInit := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelInit()

	dbpool, err := pgxpool.ConnectConfig(initializationContext, pgxConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// Verify that the server supports commit timestamps
	var trackTSOn string
	if err := dbpool.
		QueryRow(initializationContext, "SHOW track_commit_timestamp;").
		Scan(&trackTSOn); err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	watchEnabled := trackTSOn == "on"
	if !watchEnabled {
		log.Warn().Msg("watch API disabled, postgres must be run with track_commit_timestamp=on")
	}

	if config.enablePrometheusStats {
		collector := pgxpoolprometheus.NewCollector(dbpool, map[string]string{"db_name": "spicedb"})
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
		if err := common.RegisterGCMetrics(); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
	}

	gcCtx, cancelGc := context.WithCancel(context.Background())

	quantizationPeriodNanos := config.revisionQuantization.Nanoseconds()
	if quantizationPeriodNanos < 1 {
		quantizationPeriodNanos = 1
	}
	revisionQuery := fmt.Sprintf(
		querySelectRevision,
		colXID,
		tableTransaction,
		colTimestamp,
		quantizationPeriodNanos,
		colSnapshot,
	)

	validTransactionQuery := fmt.Sprintf(
		queryValidTransaction,
		colXID,
		tableTransaction,
		colTimestamp,
		config.gcWindow.Seconds(),
		colSnapshot,
	)

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	datastore := &pgDatastore{
		CachedOptimizedRevisions: revisions.NewCachedOptimizedRevisions(
			maxRevisionStaleness,
		),
		dburl:                   url,
		dbpool:                  dbpool,
		watchBufferLength:       config.watchBufferLength,
		optimizedRevisionQuery:  revisionQuery,
		validTransactionQuery:   validTransactionQuery,
		gcWindow:                config.gcWindow,
		gcInterval:              config.gcInterval,
		gcTimeout:               config.gcMaxOperationTime,
		analyzeBeforeStatistics: config.analyzeBeforeStatistics,
		usersetBatchSize:        config.splitAtUsersetCount,
		watchEnabled:            watchEnabled,
		gcCtx:                   gcCtx,
		cancelGc:                cancelGc,
		readTxOptions:           pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		maxRetries:              config.maxRetries,
	}

	datastore.SetOptimizedRevisionFunc(datastore.optimizedRevisionFunc)

	// Start a goroutine for garbage collection.
	if datastore.gcInterval > 0*time.Minute && config.gcEnabled {
		datastore.gcGroup, datastore.gcCtx = errgroup.WithContext(datastore.gcCtx)
		datastore.gcGroup.Go(func() error {
			return common.StartGarbageCollector(
				datastore.gcCtx,
				datastore,
				datastore.gcInterval,
				datastore.gcWindow,
				datastore.gcTimeout,
			)
		})
	} else {
		log.Warn().Msg("datastore background garbage collection disabled")
	}

	return datastore, nil
}

func configurePool(config postgresOptions, pgxConfig *pgxpool.Config) {
	if config.maxOpenConns != nil {
		pgxConfig.MaxConns = int32(*config.maxOpenConns)
	}

	if config.minOpenConns != nil {
		pgxConfig.MinConns = int32(*config.minOpenConns)
	}

	if pgxConfig.MaxConns > 0 && pgxConfig.MinConns > 0 && pgxConfig.MaxConns < pgxConfig.MinConns {
		log.Warn().Int32("max-connections", pgxConfig.MaxConns).Int32("min-connections", pgxConfig.MinConns).Msg("maximum number of connections configured is less than minimum number of connections; minimum will be used")
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

	pgxcommon.ConfigurePGXLogger(pgxConfig.ConnConfig)
}

type pgDatastore struct {
	*revisions.CachedOptimizedRevisions

	dburl                   string
	dbpool                  *pgxpool.Pool
	watchBufferLength       uint16
	optimizedRevisionQuery  string
	validTransactionQuery   string
	gcWindow                time.Duration
	gcInterval              time.Duration
	gcTimeout               time.Duration
	usersetBatchSize        uint16
	analyzeBeforeStatistics bool
	readTxOptions           pgx.TxOptions
	maxRetries              uint8
	watchEnabled            bool

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc
}

func (pgd *pgDatastore) SnapshotReader(revRaw datastore.Revision) datastore.Reader {
	rev := revRaw.(postgresRevision)

	createTxFunc := func(ctx context.Context) (pgx.Tx, common.TxCleanupFunc, error) {
		tx, err := pgd.dbpool.BeginTx(ctx, pgd.readTxOptions)
		if err != nil {
			return nil, nil, err
		}

		cleanup := func(ctx context.Context) {
			if err := tx.Rollback(ctx); err != nil {
				log.Ctx(ctx).Err(err).Msg("error running transaction cleanup function")
			}
		}

		return tx, cleanup, nil
	}

	querySplitter := common.TupleQuerySplitter{
		Executor:         pgxcommon.NewPGXExecutor(createTxFunc),
		UsersetBatchSize: pgd.usersetBatchSize,
	}

	return &pgReader{
		createTxFunc,
		querySplitter,
		buildLivingObjectFilterForRevision(rev),
	}
}

func noCleanup(context.Context) {}

// ReadWriteTx tarts a read/write transaction, which will be committed if no error is
// returned and rolled back if an error is returned.
func (pgd *pgDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
) (datastore.Revision, error) {
	var err error
	for i := uint8(0); i <= pgd.maxRetries; i++ {
		var newXID xid8
		var newSnapshot pgSnapshot
		err = pgd.dbpool.BeginTxFunc(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
			var err error
			newXID, newSnapshot, err = createNewTransaction(ctx, tx)
			if err != nil {
				return err
			}

			longLivedTx := func(context.Context) (pgx.Tx, common.TxCleanupFunc, error) {
				return tx, noCleanup, nil
			}

			querySplitter := common.TupleQuerySplitter{
				Executor:         pgxcommon.NewPGXExecutor(longLivedTx),
				UsersetBatchSize: pgd.usersetBatchSize,
			}

			rwt := &pgReadWriteTXN{
				&pgReader{
					longLivedTx,
					querySplitter,
					currentlyLivingObjects,
				},
				tx,
				newXID,
			}

			return fn(rwt)
		})
		if err != nil {
			if errorRetryable(err) {
				continue
			}
			return datastore.NoRevision, err
		}

		if err := newXID.MustBePresent(); err != nil {
			return datastore.NoRevision, err
		}

		return postgresRevision{newSnapshot.markComplete(newXID.Uint)}, nil
	}
	return datastore.NoRevision, fmt.Errorf("max retries exceeded: %w", err)
}

func (pgd *pgDatastore) Close() error {
	pgd.cancelGc()

	if pgd.gcGroup != nil {
		err := pgd.gcGroup.Wait()
		log.Warn().Err(err).Msg("completed shutdown of postgres datastore")
	}

	pgd.dbpool.Close()
	return nil
}

func errorRetryable(err error) bool {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		log.Debug().Err(err).Msg("couldn't determine a sqlstate error code")
		return false
	}

	// We need to check unique constraint here because some versions of postgres have an error where
	// unique constraint violations are raised instead of serialization errors.
	// (e.g. https://www.postgresql.org/message-id/flat/CAGPCyEZG76zjv7S31v_xPeLNRuzj-m%3DY2GOY7PEzu7vhB%3DyQog%40mail.gmail.com)
	return pgerr.SQLState() == pgSerializationFailure || pgerr.SQLState() == pgUniqueConstraintViolation
}

func (pgd *pgDatastore) IsReady(ctx context.Context) (bool, error) {
	headMigration, err := migrations.DatabaseMigrations.HeadRevision()
	if err != nil {
		return false, fmt.Errorf("invalid head migration found for postgres: %w", err)
	}

	currentRevision, err := migrations.NewAlembicPostgresDriver(pgd.dburl)
	if err != nil {
		return false, err
	}
	defer currentRevision.Close(ctx)

	version, err := currentRevision.Version(ctx)
	if err != nil {
		return false, err
	}

	return version == headMigration, nil
}

func (pgd *pgDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: pgd.watchEnabled}}, nil
}

func buildLivingObjectFilterForRevision(revision postgresRevision) queryFilterer {
	createdBeforeTXN := sq.Expr(fmt.Sprintf(
		snapshotAlive,
		colCreatedXid,
	), revision.snapshot, true)

	deletedAfterTXN := sq.Expr(fmt.Sprintf(
		snapshotAlive,
		colDeletedXid,
	), revision.snapshot, false)

	return func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(createdBeforeTXN).Where(deletedAfterTXN)
	}
}

func currentlyLivingObjects(original sq.SelectBuilder) sq.SelectBuilder {
	return original.Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
}

var _ datastore.Datastore = &pgDatastore{}
