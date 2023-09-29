package postgres

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/pgxpoolprometheus"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/ngrok/sqlmw"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
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

	gcBatchDeleteSize = 1000

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

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

func newPostgresDatastore(
	pgURL string,
	options ...Option,
) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// Parse the DB URI into configuration.
	parsedConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// Setup the default custom plan setting, if applicable.
	pgConfig, err := defaultCustomPlan(parsedConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// Setup the config for each of the read and write pools.
	readPoolConfig := pgConfig.Copy()
	config.readPoolOpts.ConfigurePgx(readPoolConfig)

	readPoolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		RegisterTypes(conn.TypeMap())
		return nil
	}

	writePoolConfig := pgConfig.Copy()
	config.writePoolOpts.ConfigurePgx(writePoolConfig)

	writePoolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		RegisterTypes(conn.TypeMap())
		return nil
	}

	if config.migrationPhase != "" {
		log.Info().
			Str("phase", config.migrationPhase).
			Msg("postgres configured to use intermediate migration phase")
	}

	initializationContext, cancelInit := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelInit()

	readPool, err := pgxpool.NewWithConfig(initializationContext, readPoolConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	writePool, err := pgxpool.NewWithConfig(initializationContext, writePoolConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// Verify that the server supports commit timestamps
	var trackTSOn string
	if err := readPool.
		QueryRow(initializationContext, "SHOW track_commit_timestamp;").
		Scan(&trackTSOn); err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	watchEnabled := trackTSOn == "on"
	if !watchEnabled {
		log.Warn().Msg("watch API disabled, postgres must be run with track_commit_timestamp=on")
	}

	if config.enablePrometheusStats {
		if err := prometheus.Register(pgxpoolprometheus.NewCollector(readPool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "read",
		})); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
		if err := prometheus.Register(pgxpoolprometheus.NewCollector(writePool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "write",
		})); err != nil {
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
		dburl:                   pgURL,
		readPool:                pgxcommon.MustNewInterceptorPooler(readPool, config.queryInterceptor),
		writePool:               pgxcommon.MustNewInterceptorPooler(writePool, config.queryInterceptor),
		watchBufferLength:       config.watchBufferLength,
		optimizedRevisionQuery:  revisionQuery,
		validTransactionQuery:   validTransactionQuery,
		gcWindow:                config.gcWindow,
		gcInterval:              config.gcInterval,
		gcTimeout:               config.gcMaxOperationTime,
		analyzeBeforeStatistics: config.analyzeBeforeStatistics,
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

type pgDatastore struct {
	*revisions.CachedOptimizedRevisions

	dburl                   string
	readPool, writePool     pgxcommon.ConnPooler
	watchBufferLength       uint16
	optimizedRevisionQuery  string
	validTransactionQuery   string
	gcWindow                time.Duration
	gcInterval              time.Duration
	gcTimeout               time.Duration
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

	queryFuncs := pgxcommon.QuerierFuncsFor(pgd.readPool)
	executor := common.QueryExecutor{
		Executor: pgxcommon.NewPGXExecutor(queryFuncs),
	}

	return &pgReader{
		queryFuncs,
		executor,
		buildLivingObjectFilterForRevision(rev),
	}
}

// ReadWriteTx starts a read/write transaction, which will be committed if no error is
// returned and rolled back if an error is returned.
func (pgd *pgDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)

	var err error
	for i := uint8(0); i <= pgd.maxRetries; i++ {
		var newXID xid8
		var newSnapshot pgSnapshot
		err = wrapError(pgx.BeginTxFunc(ctx, pgd.writePool, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
			var err error
			newXID, newSnapshot, err = createNewTransaction(ctx, tx)
			if err != nil {
				return err
			}

			queryFuncs := pgxcommon.QuerierFuncsFor(pgd.readPool)
			executor := common.QueryExecutor{
				Executor: pgxcommon.NewPGXExecutor(queryFuncs),
			}

			rwt := &pgReadWriteTXN{
				&pgReader{
					queryFuncs,
					executor,
					currentlyLivingObjects,
				},
				tx,
				newXID,
			}

			return fn(rwt)
		}))

		if err != nil {
			if !config.DisableRetries && errorRetryable(err) {
				pgxcommon.SleepOnErr(ctx, err, i)
				continue
			}

			return datastore.NoRevision, err
		}

		if i > 0 {
			log.Debug().Uint8("retries", i).Msg("transaction succeeded after retry")
		}

		return postgresRevision{newSnapshot.markComplete(newXID.Uint64)}, nil
	}

	if !config.DisableRetries {
		err = fmt.Errorf("max retries exceeded: %w", err)
	}

	return datastore.NoRevision, err
}

func wrapError(err error) error {
	if pgxcommon.IsSerializationError(err) {
		return common.NewSerializationError(err)
	}

	// hack: pgx asyncClose usually happens after cancellation,
	// but the reason for it being closed is not propagated
	// and all we get is attempting to perform an operation
	// on cancelled connection. This keeps the same error,
	// but wrapped along a cancellation so that:
	// - pgx logger does not log it
	// - response is sent as canceled back to the client
	if err != nil && err.Error() == "conn closed" {
		return errors.Join(err, context.Canceled)
	}

	return err
}

func (pgd *pgDatastore) Close() error {
	pgd.cancelGc()

	if pgd.gcGroup != nil {
		err := pgd.gcGroup.Wait()
		log.Warn().Err(err).Msg("completed shutdown of postgres datastore")
	}

	pgd.readPool.Close()
	pgd.writePool.Close()
	return nil
}

func errorRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if pgconn.SafeToRetry(err) {
		return true
	}

	if pgxcommon.IsSerializationError(err) {
		return true
	}

	log.Warn().Err(err).Msg("unable to determine if pgx error is retryable")
	return false
}

func (pgd *pgDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	headMigration, err := migrations.DatabaseMigrations.HeadRevision()
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("invalid head migration found for postgres: %w", err)
	}

	pgDriver, err := migrations.NewAlembicPostgresDriver(ctx, pgd.dburl)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	defer pgDriver.Close(ctx)

	version, err := pgDriver.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	if version == headMigration {
		return datastore.ReadyState{IsReady: true}, nil
	}

	return datastore.ReadyState{
		Message: fmt.Sprintf(
			"datastore is not migrated: currently at revision `%s`, but requires `%s`. Please run `spicedb migrate`.",
			version,
			headMigration,
		),
		IsReady: false,
	}, nil
}

func (pgd *pgDatastore) Features(_ context.Context) (*datastore.Features, error) {
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

// defaultCustomPlan parses a Postgres URI and determines if a plan_cache_mode
// has been specified. If not, it defaults to "force_custom_plan".
// This works around a bug impacting performance documented here:
// https://spicedb.dev/d/force-custom-plan.
func defaultCustomPlan(poolConfig *pgxpool.Config) (*pgxpool.Config, error) {
	if existing, ok := poolConfig.ConnConfig.Config.RuntimeParams["plan_cache_mode"]; ok {
		log.Info().
			Str("plan_cache_mode", existing).
			Msg("found plan_cache_mode in DB URI; leaving as-is")
		return poolConfig, nil
	}

	poolConfig.ConnConfig.Config.RuntimeParams["plan_cache_mode"] = "force_custom_plan"
	log.Warn().
		Str("details-url", "https://spicedb.dev/d/force-custom-plan").
		Str("plan_cache_mode", "force_custom_plan").
		Msg("defaulting value in Postgres DB URI")

	return poolConfig, nil
}

var _ datastore.Datastore = &pgDatastore{}
