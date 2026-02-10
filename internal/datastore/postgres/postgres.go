package postgres

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/IBM/pgxpoolprometheus"
	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mattn/go-isatty"
	"github.com/ngrok/sqlmw"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/schollz/progressbar/v3"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine = "postgres"

	errUnableToInstantiate = "unable to instantiate datastore"

	// The parameters to this format string are:
	// 1: the created_xid or deleted_xid column name
	//
	// The placeholders are the snapshot and the expected boolean value respectively.
	snapshotAlive = "pg_visible_in_snapshot(%[1]s, ?) = ?"

	// This is the largest positive integer possible in postgresql
	liveDeletedTxnID = uint64(9223372036854775807)

	tracingDriverName = "postgres-tracing"

	gcBatchDeleteSize = 1000

	primaryInstanceID = -1

	// The number of loop iterations where pg_current_xact_id is called per
	// query to the database during a repair.
	//
	// This value is ideally dependent on the underlying hardware, but 1M seems
	// to be a reasonable starting place.
	repairBatchSize = 1_000_000
)

var livingTupleConstraints = []string{"uq_relation_tuple_living_xid", "pk_relation_tuple"}

type contextKey string

const (
	ctxKeyTransactionID contextKey = "postgres_transaction_id"
)

func init() {
	dbsql.Register(tracingDriverName, sqlmw.Driver(stdlib.GetDefaultDriver(), new(traceInterceptor)))
}

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	getRevisionForGC = psql.
				Select(schema.ColXID, schema.ColSnapshot).
				From(schema.TableTransaction).
				OrderByClause(schema.ColXID + " DESC").
				Limit(1)

	createTxn = psql.Insert(schema.TableTransaction).Columns(schema.ColMetadata)

	getNow = psql.Select("NOW()")

	tracer = otel.Tracer("spicedb/internal/datastore/postgres")
)

type sqlFilter interface {
	ToSql() (string, []any, error)
}

// NewPostgresDatastore initializes a SpiceDB datastore that uses a PostgreSQL
// database by leveraging manual book-keeping to implement revisioning.
//
// This datastore is also tested to be compatible with CockroachDB.
func NewPostgresDatastore(
	ctx context.Context,
	url string,
	options ...Option,
) (datastore.Datastore, error) {
	ds, err := newPostgresDatastore(ctx, url, primaryInstanceID, options...)
	if err != nil {
		return nil, err
	}

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

// NewReadOnlyPostgresDatastore initializes a SpiceDB datastore that uses a PostgreSQL
// database by leveraging manual book-keeping to implement revisioning. This version is
// read only and does not allow for write transactions.
func NewReadOnlyPostgresDatastore(
	ctx context.Context,
	url string,
	index uint32,
	options ...Option,
) (datastore.StrictReadDatastore, error) {
	ds, err := newPostgresDatastore(ctx, url, int(index), options...)
	if err != nil {
		return nil, err
	}

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

func newPostgresDatastore(
	ctx context.Context,
	pgURL string,
	replicaIndex int,
	options ...Option,
) (datastore.StrictReadDatastore, error) {
	isPrimary := replicaIndex == primaryInstanceID
	config, err := generateConfig(options)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, pgURL)
	}

	// Parse the DB URI into configuration.
	pgConfig, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, pgURL)
	}

	// Setup the default custom plan setting, if applicable.
	// Setup the default query execution mode setting, if applicable.
	pgxcommon.ConfigureDefaultQueryExecMode(pgConfig.ConnConfig)

	// Setup the credentials provider
	var credentialsProvider datastore.CredentialsProvider
	if config.credentialsProviderName != "" {
		credentialsProvider, err = datastore.NewCredentialsProvider(ctx, config.credentialsProviderName)
		if err != nil {
			return nil, err
		}
	}

	// Setup the config for each of the read, write and GC pools.
	readPoolConfig := pgConfig.Copy()
	includeQueryParametersInTraces := config.includeQueryParametersInTraces
	err = config.readPoolOpts.ConfigurePgx(readPoolConfig, includeQueryParametersInTraces)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, pgURL)
	}

	readPoolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		RegisterTypes(conn.TypeMap())
		return nil
	}

	var writePoolConfig *pgxpool.Config
	if isPrimary {
		writePoolConfig = pgConfig.Copy()
		err = config.writePoolOpts.ConfigurePgx(writePoolConfig, includeQueryParametersInTraces)
		if err != nil {
			return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, pgURL)
		}

		writePoolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			RegisterTypes(conn.TypeMap())
			return nil
		}
	}

	if credentialsProvider != nil {
		// add before connect callbacks to trigger the token
		getToken := func(ctx context.Context, config *pgx.ConnConfig) error {
			config.User, config.Password, err = credentialsProvider.Get(ctx, fmt.Sprintf("%s:%d", config.Host, config.Port), config.User)
			return err
		}
		readPoolConfig.BeforeConnect = getToken

		if isPrimary {
			writePoolConfig.BeforeConnect = getToken
		}
	}

	if config.migrationPhase != "" {
		log.Info().
			Str("phase", config.migrationPhase).
			Msg("postgres configured to use intermediate migration phase")
	}

	initializationContext, cancelInit := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelInit()

	readPool, err := pgxpool.NewWithConfig(initializationContext, readPoolConfig)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, pgURL)
	}

	var writePool *pgxpool.Pool
	if isPrimary {
		wp, err := pgxpool.NewWithConfig(initializationContext, writePoolConfig)
		if err != nil {
			return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, pgURL)
		}
		writePool = wp
	}

	// Verify that the server supports commit timestamps
	var trackTSOn string
	if err := readPool.
		QueryRow(initializationContext, "SHOW track_commit_timestamp;").
		Scan(&trackTSOn); err != nil {
		return nil, err
	}

	watchEnabled := trackTSOn == "on" && !config.watchDisabled
	if !watchEnabled {
		if config.watchDisabled {
			log.Warn().Msg("watch API disabled via configuration")
		} else {
			log.Warn().Msg("watch API disabled, postgres must be run with track_commit_timestamp=on")
		}
	}

	collectors, err := registerAndReturnPrometheusCollectors(replicaIndex, isPrimary, readPool, writePool, config.enablePrometheusStats)
	if err != nil {
		return nil, err
	}

	headMigration, err := migrations.DatabaseMigrations.HeadRevision()
	if err != nil {
		return nil, fmt.Errorf("invalid head migration found for postgres: %w", err)
	}

	gcCtx, cancelGc := context.WithCancel(context.Background())

	quantizationPeriodNanos := config.revisionQuantization.Nanoseconds()
	if quantizationPeriodNanos < 1 {
		quantizationPeriodNanos = 1
	}

	followerReadDelayNanos := config.followerReadDelay.Nanoseconds()
	if followerReadDelayNanos < 0 {
		followerReadDelayNanos = 0
	}

	revisionQuery := fmt.Sprintf(
		querySelectRevision,
		schema.ColXID,
		schema.TableTransaction,
		schema.ColTimestamp,
		quantizationPeriodNanos,
		schema.ColSnapshot,
		followerReadDelayNanos,
	)

	var revisionHeartbeatQuery string
	if config.revisionHeartbeatEnabled {
		revisionHeartbeatQuery = fmt.Sprintf(
			insertHeartBeatRevision,
			schema.ColXID,
			schema.TableTransaction,
			schema.ColTimestamp,
			quantizationPeriodNanos,
			schema.ColSnapshot,
		)
	}

	validTransactionQuery := fmt.Sprintf(
		queryValidTransaction,
		schema.ColXID,
		schema.TableTransaction,
		schema.ColTimestamp,
		config.gcWindow.Seconds(),
		schema.ColSnapshot,
	)

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	isolationLevel := pgx.Serializable
	if config.relaxedIsolationLevel {
		isolationLevel = pgx.RepeatableRead
	}

	datastore := &pgDatastore{
		CachedOptimizedRevisions: revisions.NewCachedOptimizedRevisions(
			maxRevisionStaleness,
		),
		MigrationValidator:             common.NewMigrationValidator(headMigration, config.allowedMigrations),
		dburl:                          pgURL,
		readPool:                       pgxcommon.MustNewInterceptorPooler(readPool, config.queryInterceptor),
		writePool:                      nil, /* disabled by default */
		collectors:                     collectors,
		watchBufferLength:              config.watchBufferLength,
		watchChangeBufferMaximumSize:   config.watchChangeBufferMaximumSize,
		watchBufferWriteTimeout:        config.watchBufferWriteTimeout,
		optimizedRevisionQuery:         revisionQuery,
		validTransactionQuery:          validTransactionQuery,
		revisionHeartbeatQuery:         revisionHeartbeatQuery,
		gcWindow:                       config.gcWindow,
		gcInterval:                     config.gcInterval,
		gcTimeout:                      config.gcMaxOperationTime,
		analyzeBeforeStatistics:        config.analyzeBeforeStatistics,
		watchEnabled:                   watchEnabled,
		workerCtx:                      gcCtx,
		cancelGc:                       cancelGc,
		readTxOptions:                  pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		maxRetries:                     config.maxRetries,
		credentialsProvider:            credentialsProvider,
		isPrimary:                      isPrimary,
		inStrictReadMode:               config.readStrictMode,
		filterMaximumIDCount:           config.filterMaximumIDCount,
		schema:                         *schema.Schema(config.columnOptimizationOption, false),
		includeQueryParametersInTraces: config.includeQueryParametersInTraces,
		schemaMode:                     config.schemaMode,
		quantizationPeriodNanos:        quantizationPeriodNanos,
		isolationLevel:                 isolationLevel,
	}

	// Create schema reader/writer
	schemaReaderWriter, err := common.NewSQLSchemaReaderWriter[uint64, postgresRevision](BaseSchemaChunkerConfig, config.schemaCacheOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema reader/writer: %w", err)
	}
	datastore.schemaReaderWriter = schemaReaderWriter

	if isPrimary && config.readStrictMode {
		return nil, spiceerrors.MustBugf("strict read mode is not supported on primary instances")
	}

	if isPrimary {
		datastore.writePool = pgxcommon.MustNewInterceptorPooler(writePool, config.queryInterceptor)
	}

	datastore.SetOptimizedRevisionFunc(datastore.optimizedRevisionFunc)

	// Start a goroutine for garbage collection and the revision heartbeat.
	if isPrimary {
		datastore.workerGroup, datastore.workerCtx = errgroup.WithContext(datastore.workerCtx)
		if config.revisionHeartbeatEnabled {
			datastore.workerGroup.Go(func() error {
				return datastore.startRevisionHeartbeat(datastore.workerCtx)
			})
		}

		if datastore.gcInterval > 0*time.Minute && config.gcEnabled {
			datastore.workerGroup.Go(func() error {
				return common.StartGarbageCollector(
					datastore.workerCtx,
					datastore,
					datastore.gcInterval,
					datastore.gcWindow,
					datastore.gcTimeout,
				)
			})
		} else {
			log.Warn().Msg("datastore background garbage collection disabled")
		}
	}

	// Warm the schema cache on startup
	if err := warmSchemaCache(initializationContext, datastore); err != nil {
		log.Warn().Err(err).Msg("failed to warm schema cache on startup")
	}

	return datastore, nil
}

type pgDatastore struct {
	*revisions.CachedOptimizedRevisions
	*common.MigrationValidator

	dburl                          string
	readPool, writePool            pgxcommon.ConnPooler
	collectors                     []prometheus.Collector
	watchBufferLength              uint16
	watchChangeBufferMaximumSize   uint64
	watchBufferWriteTimeout        time.Duration
	optimizedRevisionQuery         string
	validTransactionQuery          string
	revisionHeartbeatQuery         string
	gcWindow                       time.Duration
	gcInterval                     time.Duration
	gcTimeout                      time.Duration
	analyzeBeforeStatistics        bool
	readTxOptions                  pgx.TxOptions
	maxRetries                     uint8
	watchEnabled                   bool
	isPrimary                      bool
	inStrictReadMode               bool
	schema                         common.SchemaInformation
	includeQueryParametersInTraces bool
	schemaMode                     dsoptions.SchemaMode

	credentialsProvider datastore.CredentialsProvider
	uniqueID            atomic.Pointer[string]

	workerGroup *errgroup.Group
	workerCtx   context.Context
	cancelGc    context.CancelFunc

	// SQLSchemaReaderWriter for schema operations
	schemaReaderWriter      *common.SQLSchemaReaderWriter[uint64, postgresRevision]
	gcHasRun                atomic.Bool
	filterMaximumIDCount    uint16
	quantizationPeriodNanos int64
	isolationLevel          pgx.TxIsoLevel
}

func (pgd *pgDatastore) MetricsID() (string, error) {
	return common.MetricsIDFromURL(pgd.dburl)
}

func (pgd *pgDatastore) IsStrictReadModeEnabled() bool {
	return pgd.inStrictReadMode
}

func (pgd *pgDatastore) SnapshotReader(revRaw datastore.Revision, schemaHash datastore.SchemaHash) datastore.Reader {
	rev := revRaw.(postgresRevision)

	queryFuncs := pgxcommon.QuerierFuncsFor(pgd.readPool)
	if pgd.inStrictReadMode {
		queryFuncs = strictReaderQueryFuncs{wrapped: queryFuncs, revision: rev}
	}

	executor := common.QueryRelationshipsExecutor{
		Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(queryFuncs, pgd),
	}

	return &pgReader{
		query:                queryFuncs,
		executor:             executor,
		aliveFilter:          buildLivingObjectFilterForRevision(rev),
		filterMaximumIDCount: pgd.filterMaximumIDCount,
		schema:               pgd.schema,
		schemaMode:           pgd.schemaMode,
		snapshotRevision:     rev,
		schemaReaderWriter:   pgd.schemaReaderWriter,
		schemaHash:           string(schemaHash),
	}
}

// ReadWriteTx starts a read/write transaction, which will be committed if no error is
// returned and rolled back if an error is returned.
func (pgd *pgDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	opts ...dsoptions.RWTOptionsOption,
) (datastore.Revision, error) {
	if !pgd.isPrimary {
		return datastore.NoRevision, spiceerrors.MustBugf("read-write transaction not supported on read-only datastore")
	}

	config := dsoptions.NewRWTOptionsWithOptions(opts...)

	var err error
	for i := uint8(0); i <= pgd.maxRetries; i++ {
		var newXID xid8
		var newSnapshot pgSnapshot
		var timestamp time.Time
		err = wrapError(pgx.BeginTxFunc(ctx, pgd.writePool, pgx.TxOptions{IsoLevel: pgd.isolationLevel}, func(tx pgx.Tx) error {
			var err error
			var metadata map[string]any
			if config.Metadata != nil && len(config.Metadata.GetFields()) > 0 {
				metadata = config.Metadata.AsMap()
			}

			newXID, newSnapshot, timestamp, err = createNewTransaction(ctx, tx, metadata)
			if err != nil {
				return err
			}

			queryFuncs := pgxcommon.QuerierFuncsFor(tx)
			executor := common.QueryRelationshipsExecutor{
				Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(queryFuncs, pgd),
			}

			rwt := &pgReadWriteTXN{
				pgReader: &pgReader{
					query:                queryFuncs,
					executor:             executor,
					aliveFilter:          currentlyLivingObjects,
					filterMaximumIDCount: pgd.filterMaximumIDCount,
					schema:               pgd.schema,
					schemaMode:           pgd.schemaMode,
					snapshotRevision:     datastore.NoRevision, // snapshotRevision (not yet known in RWT)
					schemaReaderWriter:   pgd.schemaReaderWriter,
					schemaHash:           string(datastore.NoSchemaHashInTransaction), // Transaction reads bypass cache
				},
				tx:     tx,
				newXID: newXID,
			}

			// Add transaction ID to context for schema operations
			ctxWithTxn := context.WithValue(ctx, ctxKeyTransactionID, newXID.Uint64)
			return fn(ctxWithTxn, rwt)
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

		nanosTimestamp, err := safecast.Convert[uint64](timestamp.UnixNano())
		if err != nil {
			return nil, spiceerrors.MustBugf("could not cast timestamp to uint64")
		}

		return postgresRevision{snapshot: newSnapshot.markComplete(newXID.Uint64), optionalTxID: newXID, optionalInexactNanosTimestamp: nanosTimestamp}, nil
	}

	if !config.DisableRetries {
		err = fmt.Errorf("max retries exceeded: %w", err)
	}

	return datastore.NoRevision, err
}

const repairTransactionIDsOperation = "transaction-ids"

func (pgd *pgDatastore) Repair(ctx context.Context, operationName string, outputProgress bool) error {
	switch operationName {
	case repairTransactionIDsOperation:
		return pgd.repairTransactionIDs(ctx, outputProgress)

	default:
		return errors.New("unknown operation")
	}
}

func (pgd *pgDatastore) repairTransactionIDs(ctx context.Context, outputProgress bool) error {
	conn, err := pgx.Connect(ctx, pgd.dburl)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Get the current transaction ID.
	currentMaximumID := 0
	if err := conn.QueryRow(ctx, queryCurrentTransactionID).Scan(&currentMaximumID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("could not get current transaction ID: %w", err)
		}
	}

	// Find the maximum transaction ID referenced in the transactions table.
	referencedMaximumID := 0
	if err := conn.QueryRow(ctx, queryLatestXID).Scan(&referencedMaximumID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("could not get maximum transaction ID: %w", err)
		}
	}

	// The delta is what this needs to fill in.
	log.Ctx(ctx).Info().Int64("current-maximum", int64(currentMaximumID)).Int64("referenced-maximum", int64(referencedMaximumID)).Msg("found transactions")
	counterDelta := referencedMaximumID - currentMaximumID
	if counterDelta < 0 {
		return nil
	}

	var bar *progressbar.ProgressBar
	if isatty.IsTerminal(os.Stderr.Fd()) && outputProgress {
		bar = progressbar.Default(int64(counterDelta), "updating transactions counter")
	}

	for i := 0; i < counterDelta; i++ {
		batchCount := min(repairBatchSize, counterDelta-i)
		if _, err := conn.Exec(ctx, queryLoopXactID(batchCount)); err != nil {
			return err
		}

		i += batchCount - 1
		if bar != nil {
			if err := bar.Add(batchCount); err != nil {
				return err
			}
		}
	}

	if bar != nil {
		if err := bar.Close(); err != nil {
			return err
		}
	}

	log.Ctx(ctx).Info().Msg("completed revisions repair")
	return nil
}

// queryLoopXactID performs pg_current_xact_id() in a server-side loop in the
// database in order to increment the xact_id.
func queryLoopXactID(batchSize int) string {
	return fmt.Sprintf(`DO $$
BEGIN
  FOR i IN 1..%d LOOP
    PERFORM pg_current_xact_id(); ROLLBACK;
  END LOOP;
END $$;`, batchSize)
}

// RepairOperations returns the available repair operations for the datastore.
func (pgd *pgDatastore) RepairOperations() []datastore.RepairOperation {
	return []datastore.RepairOperation{
		{
			Name:        repairTransactionIDsOperation,
			Description: "Brings the Postgres database up to the expected transaction ID (Postgres v15+ only)",
		},
	}
}

func wrapError(err error) error {
	// If a unique constraint violation is returned, then its likely that the cause
	// was an existing relationship given as a CREATE.
	if cerr := pgxcommon.ConvertToWriteConstraintError(livingTupleConstraints, err); cerr != nil {
		return cerr
	}

	if pgxcommon.IsSerializationError(err) {
		return common.NewSerializationError(err)
	}

	if pgxcommon.IsReadOnlyTransactionError(err) {
		return common.NewReadOnlyTransactionError(err)
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

	if pgd.workerGroup != nil {
		err := pgd.workerGroup.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn().Err(err).Msg("completed shutdown of postgres datastore")
		}
	}

	pgd.readPool.Close()

	if pgd.writePool != nil {
		pgd.writePool.Close()
	}
	for _, collector := range pgd.collectors {
		prometheus.Unregister(collector)
	}
	return nil
}

// SchemaHashReaderForTesting returns a test-only interface for reading the schema hash directly from schema_revision table.
func (pgd *pgDatastore) SchemaHashReaderForTesting() interface {
	ReadSchemaHash(ctx context.Context) (string, error)
} {
	queryFuncs := pgxcommon.QuerierFuncsFor(pgd.readPool)
	return &pgSchemaHashReaderForTesting{query: queryFuncs}
}

// SchemaHashWatcherForTesting returns a test-only interface for watching schema hash changes.
func (pgd *pgDatastore) SchemaHashWatcherForTesting() datastore.SingleStoreSchemaHashWatcher {
	queryFuncs := pgxcommon.QuerierFuncsFor(pgd.readPool)
	return newPGSchemaHashWatcher(queryFuncs)
}

type pgSchemaHashReaderForTesting struct {
	query pgxcommon.DBFuncQuerier
}

func (r *pgSchemaHashReaderForTesting) ReadSchemaHash(ctx context.Context) (string, error) {
	watcher := &pgSchemaHashWatcher{query: r.query}
	return watcher.readSchemaHash(ctx)
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
	pgDriver, err := migrations.NewAlembicPostgresDriver(ctx, pgd.dburl, pgd.credentialsProvider, pgd.includeQueryParametersInTraces)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	defer pgDriver.Close(ctx)

	version, err := pgDriver.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	state := pgd.MigrationReadyState(version)
	if !state.IsReady {
		return state, nil
	}

	// Ensure a datastore ID is present. This ensures the tables have not been truncated.
	uniqueID, err := pgd.UniqueID(ctx)
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("database validation failed: %w; if you have previously run `TRUNCATE`, this database is no longer valid and must be remigrated. See "+sharederrors.TruncateUnsupportedErrorLink, err)
	}
	log.Trace().Str("unique_id", uniqueID).Msg("postgres datastore unique ID")
	return state, nil
}

func (pgd *pgDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return pgd.OfflineFeatures()
}

func (pgd *pgDatastore) OfflineFeatures() (*datastore.Features, error) {
	continuousCheckpointing := datastore.FeatureUnsupported
	if pgd.revisionHeartbeatQuery != "" {
		continuousCheckpointing = datastore.FeatureSupported
	}

	watchStatus := datastore.FeatureUnsupported
	if pgd.watchEnabled {
		watchStatus = datastore.FeatureSupported
	}

	return &datastore.Features{
		Watch: datastore.Feature{
			Status: watchStatus,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: continuousCheckpointing,
		},
	}, nil
}

const defaultMaxHeartbeatLeaderJitterPercent = 10

func (pgd *pgDatastore) startRevisionHeartbeat(ctx context.Context) error {
	heartbeatDuration := max(time.Second, time.Nanosecond*time.Duration(pgd.quantizationPeriodNanos))
	log.Info().Stringer("interval", heartbeatDuration).Msg("starting revision heartbeat")
	tick := time.NewTicker(heartbeatDuration)

	conn, err := pgd.writePool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Leader election. Continue trying to acquire in case the current leader died.
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		ok, err := pgd.tryAcquireLock(ctx, conn, revisionHeartbeatLock)
		if err != nil {
			log.Warn().Err(err).Msg("failed to acquire revision heartbeat lock")
		}

		if ok {
			break
		}

		jitter := time.Duration(float64(heartbeatDuration) * rand.Float64() * defaultMaxHeartbeatLeaderJitterPercent / 100) // nolint:gosec
		time.Sleep(heartbeatDuration + jitter)
	}

	defer func() {
		if err := pgd.releaseLock(ctx, conn, revisionHeartbeatLock); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn().Err(err).Msg("failed to release revision heartbeat lock")
		}
	}()

	log.Info().Stringer("interval", heartbeatDuration).Msg("got elected revision heartbeat leader, starting")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			_, err := pgd.writePool.Exec(ctx, pgd.revisionHeartbeatQuery)
			if err != nil {
				log.Warn().Err(err).Msg("failed to write heartbeat revision")
			}
		}
	}
}

func buildLivingObjectFilterForRevision(revision postgresRevision) queryFilterer {
	createdBeforeTXN := sq.Expr(fmt.Sprintf(
		snapshotAlive,
		schema.ColCreatedXid,
	), revision.snapshot, true)

	deletedAfterTXN := sq.Expr(fmt.Sprintf(
		snapshotAlive,
		schema.ColDeletedXid,
	), revision.snapshot, false)

	return func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(createdBeforeTXN).Where(deletedAfterTXN)
	}
}

func currentlyLivingObjects(original sq.SelectBuilder) sq.SelectBuilder {
	return original.Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})
}

var _ datastore.Datastore = &pgDatastore{}

// warmSchemaCache attempts to warm the schema cache by loading the current schema.
// This is called during datastore initialization to avoid cold-start latency on first requests.
func warmSchemaCache(ctx context.Context, ds *pgDatastore) error {
	// Get the current revision and schema hash
	rev, schemaHash, err := ds.HeadRevision(ctx)
	if err != nil {
		return fmt.Errorf("failed to get head revision: %w", err)
	}

	// If there's no schema hash, there's no schema to warm
	if schemaHash == "" {
		log.Ctx(ctx).Debug().Msg("no schema hash found, skipping cache warming")
		return nil
	}

	// Create a simple executor for schema reading (no transaction, no revision filtering needed for warmup)
	executor := newPostgresChunkedBytesExecutor(ds.readPool.(*pgxpool.Pool))

	// Load the schema to populate the cache
	_, err = ds.schemaReaderWriter.ReadSchema(ctx, executor, rev, schemaHash)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			// Schema not found is not an error during warming - just means no schema yet
			log.Ctx(ctx).Debug().Msg("no schema found, skipping cache warming")
			return nil
		}
		return fmt.Errorf("failed to read schema: %w", err)
	}

	log.Ctx(ctx).Info().Str("schema_hash", string(schemaHash)).Msg("schema cache warmed successfully")
	return nil
}

func registerAndReturnPrometheusCollectors(replicaIndex int, isPrimary bool, readPool, writePool *pgxpool.Pool, enablePrometheusStats bool) ([]prometheus.Collector, error) {
	collectors := []prometheus.Collector{}
	if !enablePrometheusStats {
		return collectors, nil
	}

	replicaIndexStr := strconv.Itoa(replicaIndex)
	dbname := "spicedb"
	if replicaIndex != primaryInstanceID {
		dbname = "spicedb_replica_" + replicaIndexStr
	}

	readCollector := pgxpoolprometheus.NewCollector(readPool, map[string]string{
		"db_name":    dbname,
		"pool_usage": "read",
	})
	if err := prometheus.Register(readCollector); err != nil {
		// Ignore AlreadyRegisteredError which can happen in tests
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return collectors, err
		}
	} else {
		collectors = append(collectors, readCollector)
	}

	if isPrimary {
		writeCollector := pgxpoolprometheus.NewCollector(writePool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "write",
		})

		if err := prometheus.Register(writeCollector); err != nil {
			// Ignore AlreadyRegisteredError which can happen in tests
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if !errors.As(err, &alreadyRegistered) {
				return collectors, err
			}
		} else {
			collectors = append(collectors, writeCollector)
		}

		gcCollectors, err := common.RegisterGCMetrics()
		if err != nil {
			return collectors, err
		}
		collectors = append(collectors, gcCollectors...)
	}

	return collectors, nil
}
