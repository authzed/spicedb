package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/alecthomas/units"
	_ "github.com/go-sql-driver/mysql" // nolint: blank-imports
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
)

const (
	errRevision      = "unable to find revision: %w"
	errCheckRevision = "unable to check revision: %w"

	liveDeletedTxnID = uint64(9223372036854775807)

	errUnableToInstantiate = "unable to instantiate datastore: %w"

	batchDeleteSize = 1000
)

var (
	tracer = otel.Tracer("spicedb/internal/datastore/mysql")

	getNow = sb.Select("NOW(6) as now")

	sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)
)

type sqlFilter interface {
	ToSql() (string, []interface{}, error)
}

func NewMysqlDatastore(url string, options ...Option) (datastore.Datastore, error) {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("NewMysqlDatastore: failed to open database: %w", err)
	}
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	tableNamespace := tableNamespace(config.tablePrefix)
	tableTransaction := tableTransaction(config.tablePrefix)
	tableTuple := tableTuple(config.tablePrefix)

	// initialize all the statement builders
	builderCache := NewBuilderCache(tableTransaction, tableNamespace, tableTuple)

	createTxn, _, err := createTxn(tableTransaction).ToSql()
	if err != nil {
		return nil, fmt.Errorf("NewMysqlDatastore: %w", err)
	}

	gcCtx, cancelGc := context.WithCancel(context.Background())

	store := &mysqlDatastore{
		db:                        db,
		url:                       url,
		revisionFuzzingTimedelta:  config.revisionFuzzingTimedelta,
		gcWindowInverted:          -1 * config.gcWindow,
		gcInterval:                config.gcInterval,
		gcMaxOperationTime:        config.gcMaxOperationTime,
		splitAtEstimatedQuerySize: config.splitAtEstimatedQuerySize,
		gcCtx:                     gcCtx,
		cancelGc:                  cancelGc,
		watchBufferLength:         config.watchBufferLength,
		tablePrefix:               config.tablePrefix,
		createTxn:                 createTxn,
		BuilderCache:              builderCache,
		tableNamespace:            tableNamespace,
		tableTransaction:          tableTransaction,
		tableTuple:                tableTuple,
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

type mysqlDatastore struct {
	db  *sql.DB
	url string

	revisionFuzzingTimedelta  time.Duration
	gcWindowInverted          time.Duration
	gcInterval                time.Duration
	gcMaxOperationTime        time.Duration
	splitAtEstimatedQuerySize units.Base2Bytes
	watchBufferLength         uint16

	gcGroup  *errgroup.Group
	gcCtx    context.Context
	cancelGc context.CancelFunc

	tablePrefix      string
	tableNamespace   string
	tableTransaction string
	tableTuple       string

	createTxn string

	*BuilderCache
}

// Close closes the data store.
func (mds *mysqlDatastore) Close() error {
	mds.cancelGc()
	if mds.gcGroup != nil {
		if err := mds.gcGroup.Wait(); err != nil {
			log.Error().Err(err).Msg("error from running garbage collector on shutdown")
		}
	}
	return mds.db.Close()
}

func (mds *mysqlDatastore) runGarbageCollector() error {
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

func (mds *mysqlDatastore) getNow(ctx context.Context) (time.Time, error) {
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

	// RelationTupleTransaction is not timezone aware
	// Explicitly use UTC before using as a query arg
	now = now.UTC()

	return now, nil
}

func (mds *mysqlDatastore) collectGarbage() error {
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

func (mds *mysqlDatastore) collectGarbageBefore(ctx context.Context, before time.Time) (int64, int64, error) {
	// Find the highest transaction ID before the GC window.
	query, args, err := mds.GetRevision.Where(sq.Lt{common.ColTimestamp: before}).ToSql()
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

func (mds *mysqlDatastore) collectGarbageForTransaction(ctx context.Context, highest uint64) (int64, int64, error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	relCount, err := mds.batchDelete(ctx, mds.tableTuple, sq.LtOrEq{common.ColDeletedTxn: highest})
	if err != nil {
		return 0, 0, err
	}

	log.Trace().Uint64("highestTransactionId", highest).Int64("relationshipsDeleted", relCount).Msg("deleted stale relationships")
	// Delete all transaction rows with ID < the transaction ID. We don't delete the transaction
	// itself to ensure there is always at least one transaction present.
	transactionCount, err := mds.batchDelete(ctx, mds.tableTransaction, sq.Lt{common.ColID: highest})
	if err != nil {
		return relCount, 0, err
	}

	log.Trace().Uint64("highestTransactionId", highest).Int64("transactionsDeleted", transactionCount).Msg("deleted stale transactions")
	return relCount, transactionCount, nil
}

func (mds *mysqlDatastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
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

func (mds *mysqlDatastore) createNewTransaction(ctx context.Context, tx *sql.Tx) (newTxnID uint64, err error) {
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

// IsReady returns whether the datastore is ready to accept data. Datastores that require
// database schema creation will return false until the migrations have been run to create
// the necessary tables.
func (mds *mysqlDatastore) IsReady(ctx context.Context) (bool, error) {
	if err := mds.db.PingContext(ctx); err != nil {
		return false, err
	}

	driver, err := migrations.NewMysqlDriver(mds.url, mds.tablePrefix)
	if err != nil {
		return false, err
	}

	currentRevision, err := driver.Version()
	if err != nil {
		return false, err
	}

	headRevision, err := migrations.Manager.HeadRevision()
	if err != nil {
		return false, err
	}

	return headRevision == currentRevision, nil
}

// OptimizedRevision gets a revision that will likely already be replicated
// and will likely be shared amongst many queries.
func (mds *mysqlDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
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

		return common.RevisionFromTransaction(revision), nil
	}

	if upper-lower == 0 {
		return common.RevisionFromTransaction(upper), nil
	}

	return common.RevisionFromTransaction(uint64(rand.Intn(int(upper-lower))) + lower), nil
}

// HeadRevision gets a revision that is guaranteed to be at least as fresh as
// right now.
func (mds *mysqlDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "HeadRevision")
	defer span.End()

	revision, err := mds.loadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	} else if revision == 0 {
		return datastore.NoRevision, nil
	}

	return common.RevisionFromTransaction(revision), nil
}

// SeedRevision initializes the first transaction revision.
func (mds *mysqlDatastore) SeedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "SeedRevision")
	defer span.End()

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, err
	}
	defer common.LogOnError(ctx, tx.Rollback)

	txId, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, err
	}
	fmt.Printf(">>> seeded transaction %d\n", txId)
	return common.RevisionFromTransaction(txId), nil
}

// CheckRevision checks the specified revision to make sure it's valid and
// hasn't been garbage collected.
func (mds *mysqlDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	revisionTx := common.TransactionFromRevision(revision)

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
	query, args, err := mds.GetRevision.ToSql()
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

func (mds *mysqlDatastore) loadRevision(ctx context.Context) (uint64, error) {
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

	query, args, err := mds.GetRevision.ToSql()
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
		return 0, nil
	}

	return uint64(revision.Int64), nil
}

func (mds *mysqlDatastore) computeRevisionRange(ctx context.Context, windowInverted time.Duration) (uint64, uint64, error) {
	ctx, span := tracer.Start(ctx, "computeRevisionRange")
	defer span.End()

	now, err := mds.getNow(ctx)
	if err != nil {
		return 0, 0, err
	}

	span.AddEvent("DB returned value for NOW()")

	lowerBound := now.Add(windowInverted)

	query, args, err := mds.GetRevisionRange.Where(sq.GtOrEq{common.ColTimestamp: lowerBound}).ToSql()
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

func tableNamespace(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableNamespaceDefault)
}

func tableTransaction(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableTransactionDefault)
}

func tableTuple(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableTupleDefault)
}

func createTxn(tableTransaction string) sq.InsertBuilder {
	return sb.Insert(tableTransaction).Values()
}
