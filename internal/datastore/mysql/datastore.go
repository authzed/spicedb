package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
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

	errRevision             = "unable to find revision: %w"
	errCheckRevision        = "unable to check revision: %w"
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
	createBaseTxn := fmt.Sprintf("INSERT IGNORE INTO %s (id) VALUES (1)", driver.RelationTupleTransaction())

	gcCtx, cancelGc := context.WithCancel(context.Background())
	querySplitter := common.TupleQuerySplitter{
		Executor:         newMySQLExecutor(db),
		UsersetBatchSize: int(config.splitAtUsersetCount),
	}

	store := &Datastore{
		db:                       db,
		driver:                   driver,
		url:                      uri,
		revisionFuzzingTimedelta: config.revisionFuzzingTimedelta,
		gcWindowInverted:         -1 * config.gcWindow,
		gcInterval:               config.gcInterval,
		gcMaxOperationTime:       config.gcMaxOperationTime,
		gcCtx:                    gcCtx,
		cancelGc:                 cancelGc,
		watchBufferLength:        config.watchBufferLength,
		createTxn:                createTxn,
		createBaseTxn:            createBaseTxn,
		QueryBuilder:             queryBuilder,
		querySplitter:            &querySplitter,
		analyzeBeforeStats:       config.analyzeBeforeStats,
	}

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

	revisionFuzzingTimedelta time.Duration
	gcWindowInverted         time.Duration
	gcInterval               time.Duration
	gcMaxOperationTime       time.Duration
	watchBufferLength        uint16

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc

	createTxn     string
	createBaseTxn string

	*QueryBuilder
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
	if headRevision != datastore.NoRevision {
		return true, nil
	}
	return false, nil
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

	err = tx.Commit()
	if err != nil {
		return err
	}

	if lastInsertID != noLastInsertID {
		// If there was no error and `lastInsertID` is 0, the insert was ignored. This indicates the transaction
		// was already seeded by another processes (i.e. race condition).
		log.Info().Int64("headRevision", lastInsertID).Msg("seeded base datastore headRevision")
	}

	return nil
}

func (mds *Datastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	// implementation deviates slightly from PSQL implementation in order to support
	// database seeding in runtime, instead of through migrate command
	ctx, span := tracer.Start(ctx, "HeadRevision")
	defer span.End()

	revision, err := mds.loadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}
	if revision == 0 {
		return datastore.NoRevision, nil
	}

	return revisionFromTransaction(revision), nil
}

func (mds *Datastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx, span := tracer.Start(ctx, "OptimizedRevision")
	defer span.End()

	lower, upper, err := mds.computeRevisionRange(ctx, -1*mds.revisionFuzzingTimedelta)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	if errors.Is(err, sql.ErrNoRows) {
		revision, err := mds.loadRevision(ctx)
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

func (mds *Datastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	revisionTx := transactionFromRevision(revision)

	lower, upper, err := mds.computeRevisionRange(ctx, mds.gcWindowInverted)
	if err == nil {
		if revisionTx < lower {
			return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
		} else if revisionTx > upper {
			return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
		}

		return nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(errCheckRevision, err)
	}

	// There are no unexpired rows
	query, args, err := mds.GetLastRevision.ToSql()
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	var highest uint64
	err = mds.db.QueryRowContext(
		datastore.SeparateContextWithTracing(ctx), query, args...,
	).Scan(&highest)
	if errors.Is(err, sql.ErrNoRows) {
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

func (mds *Datastore) loadRevision(ctx context.Context) (uint64, error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	// slightly changed to support no revisions at all, needed for runtime seeding of first transaction
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

	query, args, err := mds.GetLastRevision.ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var revision sql.NullInt64
	err = mds.db.QueryRowContext(datastore.SeparateContextWithTracing(ctx), query, args...).Scan(&revision)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf(errRevision, err)
	}
	if !revision.Valid {
		// there are no rows in the relation tuple transaction table
		return 0, nil
	}

	return uint64(revision.Int64), nil
}

// different from PSQL's implementation - it avoids one extra query.
// this could be potentially applied to the PSQL implementation as well
func (mds *Datastore) computeRevisionRange(ctx context.Context, windowInverted time.Duration) (uint64, uint64, error) {
	ctx, span := tracer.Start(ctx, "computeRevisionRange")
	defer span.End()

	// .6f supports up to microsecond resolution window
	timestampQuery := fmt.Sprintf("%s >= TIMESTAMPADD(SECOND, %.6f, NOW(6))", colTimestamp, windowInverted.Seconds())
	query, args, err := mds.GetRevisionRange.Where(timestampQuery).Limit(1).ToSql()
	if err != nil {
		return 0, 0, err
	}

	var lower, upper sql.NullInt64
	err = mds.db.QueryRowContext(
		datastore.SeparateContextWithTracing(ctx), query, args...,
	).Scan(&lower, &upper)
	if err != nil {
		return 0, 0, err
	}

	span.AddEvent("DB returned revision range")

	if !lower.Valid || !upper.Valid {
		return 0, 0, sql.ErrNoRows
	}

	return uint64(lower.Int64), uint64(upper.Int64), nil
}

func (mds *Datastore) createNewTransaction(ctx context.Context, tx *sql.Tx) (newTxnID uint64, err error) {
	ctx, span := tracer.Start(ctx, "createNewTransaction")
	defer span.End()

	createQuery := mds.createTxn
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: %w", err)
	}

	result, err := tx.ExecContext(ctx, createQuery)
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: %w", err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: failed to get last inserted id: %w", err)
	}

	return uint64(lastInsertID), nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func revisionFromTransaction(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func transactionFromRevision(revision datastore.Revision) uint64 {
	return uint64(revision.IntPart())
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func filterToLivingObjects(original sq.SelectBuilder, revision datastore.Revision) sq.SelectBuilder {
	return original.Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
		Where(sq.Or{
			sq.Eq{colDeletedTxn: liveDeletedTxnID},
			sq.Gt{colDeletedTxn: revision},
		})
}
