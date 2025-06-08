package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	tidbCommon "github.com/authzed/spicedb/internal/datastore/tidb/common"
	"github.com/authzed/spicedb/internal/datastore/tidb/migrations"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

const (
	Engine = "tidb"

	colID               = "id"
	colTimestamp        = "timestamp"
	colMetadata         = "metadata"
	colNamespace        = "namespace"
	colConfig           = "serialized_config"
	colCreatedTxn       = "created_transaction"
	colDeletedTxn       = "deleted_transaction"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"
	colName             = "name"
	colCaveatDefinition = "definition"
	colCaveatName       = "caveat_name"
	colCaveatContext    = "caveat_context"
	colExpiration       = "expiration"

	colCounterName              = "name"
	colCounterSerializedFilter  = "serialized_filter"
	colCounterCurrentCount      = "current_count"
	colCounterUpdatedAtRevision = "count_updated_at_revision"

	errUnableToInstantiate = "unable to instantiate datastore: %w"
	liveDeletedTxnID       = uint64(math.MaxInt64)
	batchDeleteSize        = 1000
	noLastInsertID         = 0
	seedingTimeout         = 10 * time.Second

	primaryInstanceID = -1

	// https://docs.pingcap.com/tidb/stable/tidb-troubleshooting-map#error-codes
	errTiDBLockWaitTimeout = 1205 // ER_LOCK_WAIT_TIMEOUT
	errTiDBDeadlock        = 1213 // ER_LOCK_DEADLOCK
	errTiDBDuplicateEntry  = 1062 // ER_DUP_ENTRY
)

var (
	tracer = otel.Tracer("spicedb/internal/datastore/tidb")

	// Unless specified otherwise, Go's MySQL driver will assume
	// the server sends datetime in UTC,
	// see https://github.com/go-sql-driver/mysql#loc. This parameter
	// is unrelated to the session's timezone.
	// If the server's global timezone is set to something other than UTC,
	// the driver will incorrectly convert SELECT NOW(), because
	// the default session timezone is the one specified by the server.
	getNow = sb.Select("UTC_TIMESTAMP(6)")

	sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

type sqlFilter interface {
	ToSql() (string, []interface{}, error)
}

// NewTiDBDatastore creates a new tidb.Datastore value configured with the TiDB instance
// specified in through the URI parameter. Supports customization via the various options available
// in this package.
//
// URI: [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
// See https://docs.pingcap.com/tidb/stable/dev-guide-connecting-to-tidb
func NewTiDBDatastore(ctx context.Context, uri string, opts ...Option) (datastore.Datastore, error) {
	ds, err := newTiDBDatastore(ctx, uri, primaryInstanceID, opts...)
	if err != nil {
		return nil, err
	}

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

func NewReadOnlyTiDBDatastore(
	ctx context.Context,
	url string,
	index uint32,
	opts ...Option,
) (datastore.ReadOnlyDatastore, error) {
	ds, err := newTiDBDatastore(ctx, url, int(index), opts...)
	if err != nil {
		return nil, err
	}

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

func newTiDBDatastore(ctx context.Context, uri string, replicaIndex int, opts ...Option) (*Datastore, error) {
	isPrimary := replicaIndex == primaryInstanceID
	config, err := generateConfig(opts)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	tidbConfig, err := mysql.ParseDSN(uri)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, "NewTiDBDatastore: could not parse connection URI", err, uri)
	}

	if !tidbConfig.ParseTime {
		return nil, errors.New("error in NewTiDBDatastore: connection URI for TiDB datastore must include `parseTime=true` as a query parameter; see https://spicedb.dev/d/parse-time-tidb for more details")
	}

	// Setup the credentials provider
	var credentialsProvider datastore.CredentialsProvider
	if config.credentialsProviderName != "" {
		credentialsProvider, err = datastore.NewCredentialsProvider(ctx, config.credentialsProviderName)
		if err != nil {
			return nil, err
		}
	}

	err = tidbCommon.MaybeAddCredentialsProviderHook(tidbConfig, credentialsProvider)
	if err != nil {
		return nil, err
	}

	// Feed our logger through to the Connector
	tidbConfig.Logger = debugLogger{}

	// Call NewConnector with the existing parsed configuration to preserve the BeforeConnect added by the CredentialsProvider
	connector, err := mysql.NewConnector(tidbConfig)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, "NewTiDBDatastore: failed to create connector", err, uri)
	}

	if config.lockWaitTimeoutSeconds != nil {
		log.Info().Uint8("timeout", *config.lockWaitTimeoutSeconds).Msg("overriding tidb_lock_wait_timeout")
		connector, err = addSessionVariables(connector, map[string]string{
			// TiDB uses the same variable name as MySQL for lock wait timeout.
			// See: https://docs.pingcap.com/tidb/stable/system-variables#innodb_lock_wait_timeout
			"innodb_lock_wait_timeout": strconv.FormatUint(uint64(*config.lockWaitTimeoutSeconds), 10),
		})
		if err != nil {
			return nil, common.RedactAndLogSensitiveConnString(ctx, "NewTiDBDatastore: failed to add session variables to connector", err, uri)
		}
	}

	var db *sql.DB
	if config.enablePrometheusStats {
		connector, err = instrumentConnector(connector)
		if err != nil {
			return nil, common.RedactAndLogSensitiveConnString(ctx, "NewTiDBDatastore: unable to instrument connector", err, uri)
		}

		dbName := "spicedb"
		if replicaIndex != primaryInstanceID {
			dbName = fmt.Sprintf("spicedb_replica_%d", replicaIndex)
		}

		db = sql.OpenDB(connector)
		collector := sqlstats.NewStatsCollector(dbName, db)
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}

		if isPrimary {
			if err := common.RegisterGCMetrics(); err != nil {
				return nil, fmt.Errorf(errUnableToInstantiate, err)
			}
		}
	} else {
		db = sql.OpenDB(connector)
	}

	db.SetConnMaxLifetime(config.connMaxLifetime)
	db.SetConnMaxIdleTime(config.connMaxIdleTime)
	db.SetMaxOpenConns(config.maxOpenConns)
	db.SetMaxIdleConns(config.maxOpenConns)

	driver := migrations.NewTiDBDriverFromDB(db, config.tablePrefix)
	queryBuilder := NewQueryBuilder(driver)

	createTxn := sb.Insert(driver.RelationTupleTransaction()).Columns(colMetadata)

	// used for seeding the initial relation_tuple_transaction. using INSERT IGNORE on a known
	// ID value makes this idempotent (i.e. safe to execute concurrently).
	createBaseTxn := fmt.Sprintf("INSERT IGNORE INTO %s (id, timestamp) VALUES (1, FROM_UNIXTIME(1))", driver.RelationTupleTransaction())

	headMigration, err := migrations.Manager.HeadRevision()
	if err != nil {
		return nil, fmt.Errorf("invalid head migration found for tidb: %w", err)
	}

	gcCtx, cancelGc := context.WithCancel(context.Background())

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

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
		colID,
		driver.RelationTupleTransaction(),
		colTimestamp,
		quantizationPeriodNanos,
		followerReadDelayNanos,
	)

	validTransactionQuery := fmt.Sprintf(
		queryValidTransaction,
		colID,
		driver.RelationTupleTransaction(),
		colTimestamp,
		-1*config.gcWindow.Seconds(),
	)

	schema := common.NewSchemaInformationWithOptions(
		common.WithRelationshipTableName(driver.RelationTuple()),
		common.WithColNamespace(colNamespace),
		common.WithColObjectID(colObjectID),
		common.WithColRelation(colRelation),
		common.WithColUsersetNamespace(colUsersetNamespace),
		common.WithColUsersetObjectID(colUsersetObjectID),
		common.WithColUsersetRelation(colUsersetRelation),
		common.WithColCaveatName(colCaveatName),
		common.WithColCaveatContext(colCaveatContext),
		common.WithColExpiration(colExpiration),
		common.WithPaginationFilterType(common.ExpandedLogicComparison),
		common.WithPlaceholderFormat(sq.Question),
		common.WithNowFunction("NOW"), // TiDB supports NOW()
		common.WithColumnOptimization(config.columnOptimizationOption),
		common.WithExpirationDisabled(config.expirationDisabled),
		common.SetIndexes(indexes), // Assuming 'indexes' is defined elsewhere and compatible
	)

	datastore := &Datastore{
		MigrationValidator:      common.NewMigrationValidator(headMigration, config.allowedMigrations),
		db:                      db,
		driver:                  driver,
		url:                     uri,
		revisionQuantization:    config.revisionQuantization,
		gcWindow:                config.gcWindow,
		gcInterval:              config.gcInterval,
		gcTimeout:               config.gcMaxOperationTime,
		gcCtx:                   gcCtx,
		cancelGc:                cancelGc,
		watchEnabled:            !config.watchDisabled,
		watchBufferLength:       config.watchBufferLength,
		watchBufferWriteTimeout: config.watchBufferWriteTimeout,
		optimizedRevisionQuery:  revisionQuery,
		validTransactionQuery:   validTransactionQuery,
		createTxn:               createTxn,
		createBaseTxn:           createBaseTxn,
		QueryBuilder:            queryBuilder,
		readTxOptions:           &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true},
		maxRetries:              config.maxRetries,
		analyzeBeforeStats:      config.analyzeBeforeStats,
		schema:                  *schema,
		CachedOptimizedRevisions: revisions.NewCachedOptimizedRevisions(
			maxRevisionStaleness,
		),
		CommonDecoder: revisions.CommonDecoder{
			Kind: revisions.TransactionID,
		},
		filterMaximumIDCount: config.filterMaximumIDCount,
	}

	datastore.SetOptimizedRevisionFunc(datastore.optimizedRevisionFunc)

	ctxWithTimeout, cancelTimeout := context.WithTimeout(context.Background(), seedingTimeout)
	defer cancelTimeout()
	err = datastore.seedDatabase(ctxWithTimeout)
	if err != nil {
		return nil, err
	}

	// Start a goroutine for garbage collection.
	if isPrimary {
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
	}

	return datastore, nil
}

func (ds *Datastore) MetricsID() (string, error) {
	return common.MetricsIDFromURL(ds.url)
}

func (ds *Datastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	createTxFunc := func(ctx context.Context) (*sql.Tx, txCleanupFunc, error) {
		tx, err := ds.db.BeginTx(ctx, ds.readTxOptions)
		if err != nil {
			return nil, nil, err
		}

		return tx, tx.Rollback, nil
	}

	executor := common.QueryRelationshipsExecutor{
		Executor: newTiDBExecutor(ds.db, ds),
	}

	return &tidbReader{
		ds.QueryBuilder,
		createTxFunc,
		executor,
		buildLivingObjectFilterForRevision(rev),
		ds.filterMaximumIDCount,
		ds.schema,
	}
}

func noCleanup() error { return nil }

// ReadWriteTx starts a read/write transaction, which will be committed if no error is
// returned and rolled back if an error is returned.
func (ds *Datastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	rwto ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(rwto...)

	var err error
	for i := uint8(0); i <= ds.maxRetries; i++ {
		var newTxnID uint64
		if err = migrations.BeginTxFunc(ctx, ds.db, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
			var metadata map[string]any
			if config.Metadata != nil {
				metadata = config.Metadata.AsMap()
			}

			newTxnID, err = ds.createNewTransaction(ctx, tx, metadata)
			if err != nil {
				return fmt.Errorf("unable to create new txn ID: %w", err)
			}

			longLivedTx := func(context.Context) (*sql.Tx, txCleanupFunc, error) {
				return tx, noCleanup, nil
			}

			executor := common.QueryRelationshipsExecutor{
				Executor: newTiDBExecutor(tx, ds),
			}

			rwt := &tidbReadWriteTXN{
				&tidbReader{
					ds.QueryBuilder,
					longLivedTx,
					executor,
					currentlyLivingObjects,
					ds.filterMaximumIDCount,
					ds.schema,
				},
				ds.driver.RelationTuple(),
				tx,
				newTxnID,
			}

			return fn(ctx, rwt)
		}); err != nil {
			if !config.DisableRetries && isTiDBErrorRetryable(err) {
				// TODO(joel): ensure this properly retries optimistic locking failures from TiDB
				continue
			}

			return datastore.NoRevision, wrapError(err)
		}

		return revisions.NewForTransactionID(newTxnID), nil
	}
	if !config.DisableRetries {
		err = fmt.Errorf("max retries exceeded: %w", err)
	}

	return datastore.NoRevision, wrapError(err)
}

// wrapError maps any TiDB internal error into a SpiceDB typed error or an error
// that implements GRPCStatus().
func wrapError(err error) error {
	if cerr := convertToWriteConstraintError(err); cerr != nil {
		return cerr
	}
	// TODO(joel): map TiDB specific errors to common datastore errors
	return err
}

func isTiDBErrorRetryable(err error) bool {
	var mysqlerr *mysql.MySQLError
	if !errors.As(err, &mysqlerr) {
		log.Debug().Err(err).Msg("couldn't determine a sqlstate error code")
		return false
	}

	// See https://docs.pingcap.com/tidb/stable/tidb-troubleshooting-map#error-codes
	// for TiDB error codes.
	return mysqlerr.Number == errTiDBDeadlock || // 1213 ER_LOCK_DEADLOCK
		mysqlerr.Number == errTiDBLockWaitTimeout || // 1205 ER_LOCK_WAIT_TIMEOUT
		mysqlerr.Number == 9007 || // 9007 WRITE_CONFLICT_IN_OPTIMISTIC_TXN or PD_RS_NOT_LEADER
		mysqlerr.Number == 8002 || // ER_TRANS_RETRY_SELECT_FOR_UPDATE_CONFLICT
		mysqlerr.Number == 8005 || // ER_TRANS_WRITE_CONFLICT_START_TS_STALE
		mysqlerr.Number == 8022 || // ER_TRANS_COMMIT_ROLLEDBACK
		mysqlerr.Number == 8028 // ER_TRANS_SCHEMA_CHANGED_IN_TRANS
}

type querier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

type asQueryableTx struct {
	tx querier
}

func (aqt asQueryableTx) QueryFunc(ctx context.Context, f func(context.Context, common.Rows) error, sql string, args ...any) error {
	rows, err := aqt.tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return err
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return f(ctx, rows)
}

func newTiDBExecutor(tx querier, explainable datastore.Explainable) common.ExecuteReadRelsQueryFunc {
	// This implementation does not create a transaction because it's redundant for single statements, and it avoids
	// the network overhead and reduce contention on the connection pool.
	// TiDB supports autocommit by default.
	// See: https://docs.pingcap.com/tidb/stable/sql-statement-set-autocommit
	//
	// Consistent read behavior is similar to MySQL's InnoDB.
	// See: https://docs.pingcap.com/tidb/stable/transaction-isolation-levels#repeatable-read
	//
	// Prepared statements are also not used given they perform poorly on environments where connections have
	// short lifetime (e.g. to gracefully handle load-balancer connection drain)
	return func(ctx context.Context, builder common.RelationshipsQueryBuilder) (datastore.RelationshipIterator, error) {
		return common.QueryRelationships[common.Rows, structpbWrapper](ctx, builder, asQueryableTx{tx}, explainable)
	}
}

// Datastore is a TiDB-based implementation of the datastore.Datastore interface.
type Datastore struct {
	*common.MigrationValidator
	db                 *sql.DB
	driver             *migrations.TiDBDriver
	readTxOptions      *sql.TxOptions // TiDB supports Serializable isolation
	url                string
	analyzeBeforeStats bool

	revisionQuantization    time.Duration
	gcWindow                time.Duration
	gcInterval              time.Duration
	gcTimeout               time.Duration
	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration
	watchEnabled            bool
	maxRetries              uint8
	filterMaximumIDCount    uint16
	schema                  common.SchemaInformation

	optimizedRevisionQuery string
	validTransactionQuery  string

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc
	gcHasRun atomic.Bool

	createTxn     sq.InsertBuilder
	createBaseTxn string

	*QueryBuilder
	*revisions.CachedOptimizedRevisions
	revisions.CommonDecoder
}

// Close closes the data store.
func (ds *Datastore) Close() error {
	ds.cancelGc()
	if ds.gcGroup != nil {
		if err := ds.gcGroup.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("error waiting for garbage collector to shutdown")
		}
	}
	return ds.db.Close()
}

// ReadyState returns whether the datastore is ready to accept data. Datastores that require
// database schema creation will return false until the migrations have been run to create
// the necessary tables.
//
// fundamentally different from PSQL implementation:
// - checking if the current migration version is compatible is implemented with IsHeadCompatible
// - Database seeding is handled here, so that we can decouple schema migration from data migration
//   and support skeema-based migrations.
func (ds *Datastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	if err := ds.db.PingContext(ctx); err != nil {
		return datastore.ReadyState{}, err
	}

	currentMigrationRevision, err := ds.driver.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	state := ds.MigrationReadyState(currentMigrationRevision)
	if !state.IsReady {
		return state, nil
	}

	isSeeded, err := ds.isSeeded(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	if !isSeeded {
		return datastore.ReadyState{
			Message: "datastore is not properly seeded",
			IsReady: false,
		}, nil
	}
	return state, nil
}

func (ds *Datastore) Features(_ context.Context) (*datastore.Features, error) {
	return ds.OfflineFeatures()
}

func (ds *Datastore) OfflineFeatures() (*datastore.Features, error) {
	watchSupported := datastore.FeatureUnsupported
	if ds.watchEnabled {
		watchSupported = datastore.FeatureSupported
	}

	// TiDB generally supports watch, but specifics might depend on configuration or version.
	// Assuming supported for now.
	return &datastore.Features{
		Watch: datastore.Feature{
			Status: watchSupported,
		},
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureUnsupported, // Or supported if TiDB offers similar checksums
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureUnsupported, // TiDB has its own checkpointing, but maybe not exactly as SpiceDB expects
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureUnsupported, // This depends on TiDB's change data capture (CDC) behavior
		},
	}, nil
}

// isSeeded determines if the backing database has been seeded
func (ds *Datastore) isSeeded(ctx context.Context) (bool, error) {
	headRevision, err := ds.HeadRevision(ctx)
	if err != nil {
		return false, err
	}
	if headRevision == datastore.NoRevision {
		return false, nil
	}

	_, err = ds.getUniqueID(ctx) // Ensure this function is compatible with TiDB
	if err != nil {
		// Consider if specific TiDB errors need handling here
		return false, nil
	}

	return true, nil
}

// seedDatabase initializes the first transaction revision if necessary.
func (ds *Datastore) seedDatabase(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "seedDatabase")
	defer span.End()

	isSeeded, err := ds.isSeeded(ctx)
	if err != nil {
		return err
	}
	if isSeeded {
		return nil
	}

	// this seeds the transaction table with the first transaction, in a way that is idempotent
	return migrations.BeginTxFunc(ctx, ds.db, nil, func(tx *sql.Tx) error {
		// idempotent INSERT IGNORE transaction id=1. safe to be executed concurrently.
		result, err := tx.ExecContext(ctx, ds.createBaseTxn)
		if err != nil {
			return fmt.Errorf("seedDatabase: %w", err)
		}

		lastInsertID, err := result.LastInsertId()
		if err != nil {
			// TiDB might not support LastInsertId in all cases or might behave differently.
			// Check TiDB documentation for specifics.
			// If it's not supported or reliable for IGNORE, alternative check might be needed.
			return fmt.Errorf("seedDatabase: failed to get last inserted id: %w", err)
		}

		if lastInsertID != noLastInsertID {
			log.Ctx(ctx).Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore headRevision")
		}

		uuidSQL, uuidArgs, err := sb.
			Insert(ds.driver.Metadata()).
			Options("IGNORE"). // TiDB supports INSERT IGNORE
			Columns(metadataIDColumn, metadataUniqueIDColumn).
			Values(0, uuid.NewString()).
			ToSql()
		if err != nil {
			return fmt.Errorf("seedDatabase: failed to prepare SQL: %w", err)
		}

		insertUniqueResult, err := tx.ExecContext(ctx, uuidSQL, uuidArgs...)
		if err != nil {
			return fmt.Errorf("seedDatabase: failed to insert unique ID: %w", err)
		}

		lastInsertID, err = insertUniqueResult.LastInsertId()
		if err != nil {
			return fmt.Errorf("seedDatabase: failed to get last inserted unique id: %w", err)
		}

		if lastInsertID != noLastInsertID {
			log.Ctx(ctx).Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore unique ID")
		}

		return nil
	})
}

func buildLivingObjectFilterForRevision(revision datastore.Revision) queryFilterer {
	revID := revision.(revisions.TransactionIDRevision).TransactionID()
	return func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(sq.LtOrEq{colCreatedTxn: revID}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: revID},
			})
	}
}

func currentlyLivingObjects(original sq.SelectBuilder) sq.SelectBuilder {
	return original.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
}

type debugLogger struct{}

// Print logs messages from the MySQL driver.
func (debugLogger) Print(v ...any) {
	log.Logger.Debug().CallerSkipFrame(1).Str("datastore", "tidb").Msg(fmt.Sprint(v...))
}
