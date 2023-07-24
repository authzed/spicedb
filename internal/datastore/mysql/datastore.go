package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	Engine = "mysql"

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
	colName             = "name"
	colCaveatDefinition = "definition"
	colCaveatName       = "caveat_name"
	colCaveatContext    = "caveat_context"

	errUnableToInstantiate = "unable to instantiate datastore: %w"
	liveDeletedTxnID       = uint64(math.MaxInt64)
	batchDeleteSize        = 1000
	noLastInsertID         = 0
	seedingTimeout         = 10 * time.Second

	// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_lock_wait_timeout
	errMysqlLockWaitTimeout = 1205

	// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_lock_deadlock
	errMysqlDeadlock = 1213

	// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_dup_entry
	errMysqlDuplicateEntry = 1062
)

var (
	tracer = otel.Tracer("spicedb/internal/datastore/mysql")

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

// NewMySQLDatastore creates a new mysql.Datastore value configured with the MySQL instance
// specified in through the URI parameter. Supports customization via the various options available
// in this package.
//
// URI: [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
// See https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
func NewMySQLDatastore(uri string, options ...Option) (datastore.Datastore, error) {
	ds, err := newMySQLDatastore(uri, options...)
	if err != nil {
		return nil, err
	}

	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

func newMySQLDatastore(uri string, options ...Option) (*Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	parsedURI, err := mysql.ParseDSN(uri)
	if err != nil {
		return nil, fmt.Errorf("NewMySQLDatastore: could not parse connection URI `%s`: %w", uri, err)
	}

	if !parsedURI.ParseTime {
		return nil, fmt.Errorf("NewMySQLDatastore: connection URI for MySQL datastore must include `parseTime=true` as a query parameter. See https://spicedb.dev/d/parse-time-mysql for more details. Found: `%s`", uri)
	}

	connector, err := mysql.MySQLDriver{}.OpenConnector(uri)
	if err != nil {
		return nil, fmt.Errorf("NewMySQLDatastore: failed to create connector: %w", err)
	}

	if config.lockWaitTimeoutSeconds != nil {
		log.Info().Uint8("timeout", *config.lockWaitTimeoutSeconds).Msg("overriding innodb_lock_wait_timeout")
		connector, err = addSessionVariables(connector, map[string]string{
			"innodb_lock_wait_timeout": strconv.FormatUint(uint64(*config.lockWaitTimeoutSeconds), 10),
		})
		if err != nil {
			return nil, fmt.Errorf("NewMySQLDatastore: failed to add session variables to connector: %w", err)
		}
	}

	var db *sql.DB
	if config.enablePrometheusStats {
		connector, err = instrumentConnector(connector)
		if err != nil {
			return nil, fmt.Errorf("NewMySQLDatastore: unable to instrument connector: %w", err)
		}

		db = sql.OpenDB(connector)
		collector := sqlstats.NewStatsCollector("spicedb", db)
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}

		if err := common.RegisterGCMetrics(); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
	} else {
		db = sql.OpenDB(connector)
	}

	db.SetConnMaxLifetime(config.connMaxLifetime)
	db.SetConnMaxIdleTime(config.connMaxIdleTime)
	db.SetMaxOpenConns(config.maxOpenConns)
	db.SetMaxIdleConns(config.maxOpenConns)

	driver := migrations.NewMySQLDriverFromDB(db, config.tablePrefix)
	queryBuilder := NewQueryBuilder(driver)

	createTxn, _, err := sb.Insert(driver.RelationTupleTransaction()).Values().ToSql()
	if err != nil {
		return nil, fmt.Errorf("NewMySQLDatastore: %w", err)
	}

	// used for seeding the initial relation_tuple_transaction. using INSERT IGNORE on a known
	// ID value makes this idempotent (i.e. safe to execute concurrently).
	createBaseTxn := fmt.Sprintf("INSERT IGNORE INTO %s (id, timestamp) VALUES (1, FROM_UNIXTIME(1))", driver.RelationTupleTransaction())

	gcCtx, cancelGc := context.WithCancel(context.Background())

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	quantizationPeriodNanos := config.revisionQuantization.Nanoseconds()
	if quantizationPeriodNanos < 1 {
		quantizationPeriodNanos = 1
	}

	revisionQuery := fmt.Sprintf(
		querySelectRevision,
		colID,
		driver.RelationTupleTransaction(),
		colTimestamp,
		quantizationPeriodNanos,
	)

	validTransactionQuery := fmt.Sprintf(
		queryValidTransaction,
		colID,
		driver.RelationTupleTransaction(),
		colTimestamp,
		-1*config.gcWindow.Seconds(),
	)

	store := &Datastore{
		db:                     db,
		driver:                 driver,
		url:                    uri,
		revisionQuantization:   config.revisionQuantization,
		gcWindow:               config.gcWindow,
		gcInterval:             config.gcInterval,
		gcTimeout:              config.gcMaxOperationTime,
		gcCtx:                  gcCtx,
		cancelGc:               cancelGc,
		watchBufferLength:      config.watchBufferLength,
		optimizedRevisionQuery: revisionQuery,
		validTransactionQuery:  validTransactionQuery,
		createTxn:              createTxn,
		createBaseTxn:          createBaseTxn,
		QueryBuilder:           queryBuilder,
		readTxOptions:          &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true},
		maxRetries:             config.maxRetries,
		analyzeBeforeStats:     config.analyzeBeforeStats,
		CachedOptimizedRevisions: revisions.NewCachedOptimizedRevisions(
			maxRevisionStaleness,
		),
	}

	store.SetOptimizedRevisionFunc(store.optimizedRevisionFunc)

	ctx, cancel := context.WithTimeout(context.Background(), seedingTimeout)
	defer cancel()
	err = store.seedDatabase(ctx)
	if err != nil {
		return nil, err
	}

	// Start a goroutine for garbage collection.
	if store.gcInterval > 0*time.Minute && config.gcEnabled {
		store.gcGroup, store.gcCtx = errgroup.WithContext(store.gcCtx)
		store.gcGroup.Go(func() error {
			return common.StartGarbageCollector(
				store.gcCtx,
				store,
				store.gcInterval,
				store.gcWindow,
				store.gcTimeout,
			)
		})
	} else {
		log.Warn().Msg("datastore background garbage collection disabled")
	}

	return store, nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) SnapshotReader(revisionRaw datastore.Revision) datastore.Reader {
	rev := revisionRaw.(revision.Decimal)

	createTxFunc := func(ctx context.Context) (*sql.Tx, txCleanupFunc, error) {
		tx, err := mds.db.BeginTx(ctx, mds.readTxOptions)
		if err != nil {
			return nil, nil, err
		}

		return tx, tx.Rollback, nil
	}

	executor := common.QueryExecutor{
		Executor: newMySQLExecutor(mds.db),
	}

	return &mysqlReader{
		mds.QueryBuilder,
		createTxFunc,
		executor,
		buildLivingObjectFilterForRevision(rev),
	}
}

func noCleanup() error { return nil }

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// ReadWriteTx starts a read/write transaction, which will be committed if no error is
// returned and rolled back if an error is returned.
func (mds *Datastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)

	var err error
	for i := uint8(0); i <= mds.maxRetries; i++ {
		var newTxnID uint64
		if err = migrations.BeginTxFunc(ctx, mds.db, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
			newTxnID, err = mds.createNewTransaction(ctx, tx)
			if err != nil {
				return fmt.Errorf("unable to create new txn ID: %w", err)
			}

			longLivedTx := func(context.Context) (*sql.Tx, txCleanupFunc, error) {
				return tx, noCleanup, nil
			}

			executor := common.QueryExecutor{
				Executor: newMySQLExecutor(tx),
			}

			rwt := &mysqlReadWriteTXN{
				&mysqlReader{
					mds.QueryBuilder,
					longLivedTx,
					executor,
					currentlyLivingObjects,
				},
				mds.driver.RelationTuple(),
				tx,
				newTxnID,
			}

			return fn(rwt)
		}); err != nil {
			if !config.DisableRetries && isErrorRetryable(err) {
				continue
			}

			return datastore.NoRevision, err
		}

		return revisionFromTransaction(newTxnID), nil
	}
	if !config.DisableRetries {
		err = fmt.Errorf("max retries exceeded: %w", err)
	}
	return datastore.NoRevision, err
}

func isErrorRetryable(err error) bool {
	var mysqlerr *mysql.MySQLError
	if !errors.As(err, &mysqlerr) {
		log.Debug().Err(err).Msg("couldn't determine a sqlstate error code")
		return false
	}

	return mysqlerr.Number == errMysqlDeadlock || mysqlerr.Number == errMysqlLockWaitTimeout
}

type querier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

func newMySQLExecutor(tx querier) common.ExecuteQueryFunc {
	// This implementation does not create a transaction because it's redundant for single statements, and it avoids
	// the network overhead and reduce contention on the connection pool. From MySQL docs:
	//
	// https://dev.mysql.com/doc/refman/5.7/en/commit.html
	// "By default, MySQL runs with autocommit mode enabled. This means that, when not otherwise inside a transaction,
	// each statement is atomic, as if it were surrounded by START TRANSACTION and COMMIT."
	//
	// https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
	// "Consistent read is the default mode in which InnoDB processes SELECT statements in READ COMMITTED and
	// REPEATABLE READ isolation levels. A consistent read does not set any locks on the tables it accesses,
	// and therefore other sessions are free to modify those tables at the same time a consistent read
	// is being performed on the table."
	//
	// Prepared statements are also not used given they perform poorly on environments where connections have
	// short lifetime (e.g. to gracefully handle load-balancer connection drain)
	return func(ctx context.Context, sqlQuery string, args []interface{}) ([]*core.RelationTuple, error) {
		span := trace.SpanFromContext(ctx)

		rows, err := tx.QueryContext(ctx, sqlQuery, args...)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}
		defer common.LogOnError(ctx, rows.Close)

		span.AddEvent("Query issued to database")

		var tuples []*core.RelationTuple
		for rows.Next() {
			nextTuple := &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{},
				Subject:             &core.ObjectAndRelation{},
			}

			var caveatName string
			var caveatContext caveatContextWrapper
			err := rows.Scan(
				&nextTuple.ResourceAndRelation.Namespace,
				&nextTuple.ResourceAndRelation.ObjectId,
				&nextTuple.ResourceAndRelation.Relation,
				&nextTuple.Subject.Namespace,
				&nextTuple.Subject.ObjectId,
				&nextTuple.Subject.Relation,
				&caveatName,
				&caveatContext,
			)
			if err != nil {
				return nil, fmt.Errorf(errUnableToQueryTuples, err)
			}

			nextTuple.Caveat, err = common.ContextualizedCaveatFrom(caveatName, caveatContext)
			if err != nil {
				return nil, fmt.Errorf(errUnableToQueryTuples, err)
			}

			tuples = append(tuples, nextTuple)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}
		span.AddEvent("Tuples loaded", trace.WithAttributes(attribute.Int("tupleCount", len(tuples))))
		return tuples, nil
	}
}

// Datastore is a MySQL-based implementation of the datastore.Datastore interface
type Datastore struct {
	db                 *sql.DB
	driver             *migrations.MySQLDriver
	readTxOptions      *sql.TxOptions
	url                string
	analyzeBeforeStats bool

	revisionQuantization time.Duration
	gcWindow             time.Duration
	gcInterval           time.Duration
	gcTimeout            time.Duration
	watchBufferLength    uint16
	maxRetries           uint8

	optimizedRevisionQuery string
	validTransactionQuery  string

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc

	createTxn     string
	createBaseTxn string

	*QueryBuilder
	*revisions.CachedOptimizedRevisions
	revision.DecimalDecoder
}

// Close closes the data store.
func (mds *Datastore) Close() error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	mds.cancelGc()
	if mds.gcGroup != nil {
		if err := mds.gcGroup.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("error waiting for garbage collector to shutdown")
		}
	}
	return mds.db.Close()
}

// ReadyState returns whether the datastore is ready to accept data. Datastores that require
// database schema creation will return false until the migrations have been run to create
// the necessary tables.
//
// fundamentally different from PSQL implementation:
//   - checking if the current migration version is compatible is implemented with IsHeadCompatible
//   - Database seeding is handled here, so that we can decouple schema migration from data migration
//     and support skeema-based migrations.
func (mds *Datastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	if err := mds.db.PingContext(ctx); err != nil {
		return datastore.ReadyState{}, err
	}

	currentMigrationRevision, err := mds.driver.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	compatible, err := migrations.Manager.IsHeadCompatible(currentMigrationRevision)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	if !compatible {
		return datastore.ReadyState{
			Message: "datastore is not at a currently compatible revision",
			IsReady: false,
		}, nil
	}

	isSeeded, err := mds.isSeeded(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	if !isSeeded {
		return datastore.ReadyState{
			Message: "datastore is not properly seeded",
			IsReady: false,
		}, nil
	}

	return datastore.ReadyState{
		Message: "",
		IsReady: true,
	}, nil
}

func (mds *Datastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

// isSeeded determines if the backing database has been seeded
func (mds *Datastore) isSeeded(ctx context.Context) (bool, error) {
	headRevision, err := mds.HeadRevision(ctx)
	if err != nil {
		return false, err
	}
	if headRevision == datastore.NoRevision {
		return false, nil
	}

	_, err = mds.getUniqueID(ctx)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// seedDatabase initializes the first transaction revision if necessary.
func (mds *Datastore) seedDatabase(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "seedDatabase")
	defer span.End()

	isSeeded, err := mds.isSeeded(ctx)
	if err != nil {
		return err
	}
	if isSeeded {
		return nil
	}

	// this seeds the transaction table with the first transaction, in a way that is idempotent
	return migrations.BeginTxFunc(ctx, mds.db, nil, func(tx *sql.Tx) error {
		// idempotent INSERT IGNORE transaction id=1. safe to be executed concurrently.
		result, err := tx.ExecContext(ctx, mds.createBaseTxn)
		if err != nil {
			return fmt.Errorf("seedDatabase: %w", err)
		}

		lastInsertID, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("seedDatabase: failed to get last inserted id: %w", err)
		}

		if lastInsertID != noLastInsertID {
			// If there was no error and `lastInsertID` is 0, the insert was ignored. This indicates the transaction
			// was already seeded by another processes (i.e. race condition).
			log.Ctx(ctx).Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore headRevision")
		}

		uuidSQL, uuidArgs, err := sb.
			Insert(mds.driver.Metadata()).
			Options("IGNORE").
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
			// If there was no error and `lastInsertID` is 0, the insert was ignored. This indicates the transaction
			// was already seeded by another processes (i.e. race condition).
			log.Ctx(ctx).Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore unique ID")
		}

		return nil
	})
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func buildLivingObjectFilterForRevision(revision revision.Decimal) queryFilterer {
	return func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: revision},
			})
	}
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func currentlyLivingObjects(original sq.SelectBuilder) sq.SelectBuilder {
	return original.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
}
