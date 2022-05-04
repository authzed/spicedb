package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
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

	errUnableToInstantiate  = "unable to instantiate datastore: %w"
	errUnableToQueryTuples  = "unable to query tuples: %w"
	errUnableToWriteTuples  = "unable to write tuples: %w"
	errUnableToDeleteTuples = "unable to delete tuples: %w"
	liveDeletedTxnID        = uint64(9223372036854775807)
	batchDeleteSize         = 1000
	noLastInsertID          = 0
	seedingTimeout          = 10 * time.Second
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
func NewMySQLDatastore(uri string, options ...Option) (*Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	connector, err := mysql.MySQLDriver{}.OpenConnector(uri)
	if err != nil {
		return nil, fmt.Errorf("NewMySQLDatastore: failed to create connector: %w", err)
	}
	var db *sql.DB
	if config.enablePrometheusStats {
		connector, err = instrumentConnector(connector)
		if err != nil {
			return nil, fmt.Errorf("NewMySQLDatastore: unable to instrument connector: %w", err)
		}

		db = sql.OpenDB(connector)
		collector := sqlstats.NewStatsCollector("spicedb", db)
		err := prometheus.Register(collector)
		if err != nil {
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
	querySplitter := common.TupleQuerySplitter{
		Executor:         newMySQLExecutor(db),
		UsersetBatchSize: config.splitAtUsersetCount,
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	quantizationPeriodNanos := config.revisionQuantization.Nanoseconds()
	if quantizationPeriodNanos < 1 {
		quantizationPeriodNanos = 1
	}

	gcWindowInverted := -1 * config.gcWindow

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
		gcWindowInverted.Seconds(),
	)

	store := &Datastore{
		db:                     db,
		driver:                 driver,
		url:                    uri,
		revisionQuantization:   config.revisionQuantization,
		gcWindowInverted:       gcWindowInverted,
		gcInterval:             config.gcInterval,
		gcMaxOperationTime:     config.gcMaxOperationTime,
		gcCtx:                  gcCtx,
		cancelGc:               cancelGc,
		watchBufferLength:      config.watchBufferLength,
		optimizedRevisionQuery: revisionQuery,
		validTransactionQuery:  validTransactionQuery,
		createTxn:              createTxn,
		createBaseTxn:          createBaseTxn,
		QueryBuilder:           queryBuilder,
		querySplitter:          &querySplitter,
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
	if store.gcInterval > 0*time.Minute {
		store.gcGroup, store.gcCtx = errgroup.WithContext(store.gcCtx)
		store.gcGroup.Go(store.runGarbageCollector)
	} else {
		log.Warn().Msg("garbage collection disabled in mysql driver")
	}

	return store, nil
}

func newMySQLExecutor(db *sql.DB) common.ExecuteQueryFunc {
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
	return func(ctx context.Context, revision datastore.Revision, sqlQuery string, args []interface{}) ([]*core.RelationTuple, error) {
		ctx = datastore.SeparateContextWithTracing(ctx)

		span := trace.SpanFromContext(ctx)

		rows, err := db.QueryContext(ctx, sqlQuery, args...)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}
		defer migrations.LogOnError(ctx, rows.Close)

		span.AddEvent("Query issued to database")

		var tuples []*core.RelationTuple
		for rows.Next() {
			nextTuple := &core.RelationTuple{
				ObjectAndRelation: &core.ObjectAndRelation{},
				User: &core.User{
					UserOneof: &core.User_Userset{
						Userset: &core.ObjectAndRelation{},
					},
				},
			}
			userset := nextTuple.User.GetUserset()
			err := rows.Scan(
				&nextTuple.ObjectAndRelation.Namespace,
				&nextTuple.ObjectAndRelation.ObjectId,
				&nextTuple.ObjectAndRelation.Relation,
				&userset.Namespace,
				&userset.ObjectId,
				&userset.Relation,
			)
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
	querySplitter      *common.TupleQuerySplitter
	url                string
	analyzeBeforeStats bool

	revisionQuantization time.Duration
	gcWindowInverted     time.Duration
	gcInterval           time.Duration
	gcMaxOperationTime   time.Duration
	watchBufferLength    uint16

	optimizedRevisionQuery string
	validTransactionQuery  string

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc

	createTxn     string
	createBaseTxn string

	*QueryBuilder
	*revisions.CachedOptimizedRevisions
}

// Close closes the data store.
func (mds *Datastore) Close() error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	mds.cancelGc()
	if mds.gcGroup != nil {
		if err := mds.gcGroup.Wait(); err != nil {
			log.Error().Err(err).Msg("error waiting for garbage collector to shutdown")
		}
	}
	return mds.db.Close()
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) runGarbageCollector() error {
	log.Info().Dur("interval", mds.gcInterval).Msg("garbage collection worker started for mysql driver")

	for {
		select {
		case <-mds.gcCtx.Done():
			log.Info().Msg("shutting down garbage collection worker for mysql driver")
			return mds.gcCtx.Err()

		case <-time.After(mds.gcInterval):
			err := mds.collectGarbage()
			if err != nil {
				log.Warn().Err(err).Msg("error when attempting to perform garbage collection")
			}
		}
	}
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) getNow(ctx context.Context) (time.Time, error) {
	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Now(), err
	}

	var now time.Time
	err = mds.db.QueryRowContext(datastore.SeparateContextWithTracing(ctx), nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Now(), err
	}

	// Just for convenience while debugging - MySQL and the driver do properly handle timezones
	now = now.UTC()

	return now, nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - this implementation does not have metrics yet
// - an additional useful logging message is added
// - context is removed from logger because zerolog expects logger in the context
func (mds *Datastore) collectGarbage() error {
	ctx, cancel := context.WithTimeout(context.Background(), mds.gcMaxOperationTime)
	defer cancel()

	// Ensure the database is ready.
	ready, err := mds.IsReady(ctx)
	if err != nil {
		return err
	}

	if !ready {
		log.Warn().Msg("cannot perform mysql garbage collection: mysql driver is not yet ready")
		return nil
	}

	now, err := mds.getNow(ctx)
	if err != nil {
		return err
	}

	before := now.Add(mds.gcWindowInverted)
	log.Debug().Time("before", before).Msg("running mysql garbage collection")
	relCount, transCount, err := mds.collectGarbageBefore(ctx, before)
	log.Debug().Int64("relationshipsDeleted", relCount).Int64("transactionsDeleted", transCount).Msg("garbage collection completed for mysql")
	return err
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - main difference is how the PSQL driver handles null values
func (mds *Datastore) collectGarbageBefore(ctx context.Context, before time.Time) (int64, int64, error) {
	// Find the highest transaction ID before the GC window.
	query, args, err := mds.GetLastRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	var value sql.NullInt64
	err = mds.db.QueryRowContext(
		datastore.SeparateContextWithTracing(ctx), query, args...,
	).Scan(&value)
	if err != nil {
		return 0, 0, err
	}

	if !value.Valid {
		log.Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return 0, 0, nil
	}
	highest := uint64(value.Int64)

	log.Trace().Uint64("highestTransactionId", highest).Msg("retrieved transaction ID for GC")

	return mds.collectGarbageForTransaction(ctx, highest)
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - implementation misses metrics
func (mds *Datastore) collectGarbageForTransaction(ctx context.Context, highest uint64) (int64, int64, error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	relCount, err := mds.batchDelete(ctx, mds.driver.RelationTuple(), sq.LtOrEq{colDeletedTxn: highest})
	if err != nil {
		return 0, 0, err
	}

	log.Trace().Uint64("highestTransactionId", highest).Int64("relationshipsDeleted", relCount).Msg("deleted stale relationships")

	// Delete all transaction rows with ID < the transaction ID. We don't delete the transaction
	// itself to ensure there is always at least one transaction present.
	transactionCount, err := mds.batchDelete(ctx, mds.driver.RelationTupleTransaction(), sq.Lt{colID: highest})
	if err != nil {
		return relCount, 0, err
	}

	log.Trace().Uint64("highestTransactionId", highest).Int64("transactionsDeleted", transactionCount).Msg("deleted stale transactions")
	return relCount, transactionCount, nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - query was reworked to make it compatible with Vitess
// - API differences with PSQL driver
func (mds *Datastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	query, args, err := sb.Delete(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	var deletedCount int64
	for {
		cr, err := mds.db.ExecContext(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted, err := cr.RowsAffected()
		if err != nil {
			return deletedCount, err
		}
		deletedCount += rowsDeleted
		if rowsDeleted < batchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}

// IsReady returns whether the datastore is ready to accept data. Datastores that require
// database schema creation will return false until the migrations have been run to create
// the necessary tables.
//
// fundamentally different from PSQL implementation:
// - checking if the current migration version is compatible is implemented with IsHeadCompatible
// - Database seeding is handled here, so that we can decouple schema migration from data migration
//   and support skeema-based migrations.
func (mds *Datastore) IsReady(ctx context.Context) (bool, error) {
	if err := mds.db.PingContext(ctx); err != nil {
		return false, err
	}

	currentMigrationRevision, err := mds.driver.Version()
	if err != nil {
		return false, err
	}

	compatible, err := migrations.Manager.IsHeadCompatible(currentMigrationRevision)
	if err != nil {
		return false, err
	}
	if !compatible {
		return false, nil
	}

	isSeeded, err := mds.isSeeded(ctx)
	if err != nil {
		return false, err
	}
	if !isSeeded {
		return false, nil
	}

	return true, nil
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
	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

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
		log.Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore headRevision")
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
		log.Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore unique ID")
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func filterToLivingObjects(original sq.SelectBuilder, revision datastore.Revision) sq.SelectBuilder {
	return original.Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
		Where(sq.Or{
			sq.Eq{colDeletedTxn: liveDeletedTxnID},
			sq.Gt{colDeletedTxn: revision},
		})
}
