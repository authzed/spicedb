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
	"github.com/jmoiron/sqlx"
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

	createTxn = "INSERT INTO relation_tuple_transaction VALUES()"

	liveDeletedTxnID = uint64(9223372036854775807)

	errUnableToInstantiate = "unable to instantiate datastore: %w"

	batchDeleteSize = 1000
)

var (
	tracer = otel.Tracer("spicedb/internal/datastore/mysql")

	getRevision      = sb.Select("MAX(id)").From(common.TableTransaction)
	getRevisionRange = sb.Select("MIN(id)", "MAX(id)").From(common.TableTransaction)

	getNow = sb.Select("NOW(6) as now")

	sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)
)

type sqlFilter interface {
	ToSql() (string, []interface{}, error)
}

func NewMysqlDatastore(url string, options ...Option) (datastore.Datastore, error) {
	db, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("NewMysqlDatastore: failed to open database: %w", err)
	}
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	gcCtx, cancelGc := context.WithCancel(context.Background())

	datastore := &mysqlDatastore{
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
	}

	// Start a goroutine for garbage collection.
	if datastore.gcInterval > 0*time.Minute {
		datastore.gcGroup, datastore.gcCtx = errgroup.WithContext(datastore.gcCtx)
		datastore.gcGroup.Go(datastore.runGarbageCollector)
	} else {
		log.Warn().Msg("garbage collection disabled in mysql driver")
	}

	return datastore, nil
}

type mysqlDatastore struct {
	db  *sqlx.DB
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
}

// Close closes the data store.
func (mds *mysqlDatastore) Close() error {
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
			} else {
				log.Debug().Msg("garbage collection completed for mysql")
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
	err = mds.db.QueryRowxContext(datastore.SeparateContextWithTracing(ctx), nowSQL, nowArgs...).Scan(&now)
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
		log.Ctx(ctx).Warn().Msg("cannot perform mysql garbage collection: mysql driver is not yet ready")
		return nil
	}

	now, err := mds.getNow(ctx)
	if err != nil {
		return err
	}

	before := now.Add(mds.gcWindowInverted)
	log.Ctx(ctx).Debug().Time("before", before).Msg("running mysql garbage collection")
	_, _, err = mds.collectGarbageBefore(ctx, before)
	return err
}

func (mds *mysqlDatastore) collectGarbageBefore(ctx context.Context, before time.Time) (int64, int64, error) {
	// Find the highest transaction ID before the GC window.
	query, args, err := getRevision.Where(sq.Lt{common.ColTimestamp: before}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	var value sql.NullInt64
	err = mds.db.QueryRowxContext(
		datastore.SeparateContextWithTracing(ctx), query, args...,
	).Scan(&value)
	if err != nil {
		return 0, 0, err
	}

	if !value.Valid {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return 0, 0, nil
	}

	highest := uint64(value.Int64)
	if err != nil {
		return 0, 0, err
	}

	log.Ctx(ctx).Trace().Uint64("highestTransactionId", highest).Msg("retrieved transaction ID for GC")

	return mds.collectGarbageForTransaction(ctx, highest)
}

func (mds *mysqlDatastore) collectGarbageForTransaction(ctx context.Context, highest uint64) (int64, int64, error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	relCount, err := mds.batchDelete(ctx, common.TableTuple, sq.LtOrEq{common.ColDeletedTxn: highest})
	if err != nil {
		return 0, 0, err
	}

	log.Ctx(ctx).Trace().Uint64("highestTransactionId", highest).Int64("relationshipsDeleted", relCount).Msg("deleted stale relationships")
	// Delete all transaction rows with ID < the transaction ID. We don't delete the transaction
	// itself to ensure there is always at least one transaction present.
	transactionCount, err := mds.batchDelete(ctx, common.TableTransaction, sq.Lt{common.ColID: highest})
	if err != nil {
		return relCount, 0, err
	}

	log.Ctx(ctx).Trace().Uint64("highestTransactionId", highest).Int64("transactionsDeleted", transactionCount).Msg("deleted stale transactions")
	return relCount, transactionCount, nil
}

func (mds *mysqlDatastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	sql, args, err := sb.Select("id").From(tableName).Where(filter).ToSql()
	if err != nil {
		return -1, err
	}

	query := fmt.Sprintf(`WITH rows AS (%s)
		  DELETE FROM %s
		  WHERE id IN (SELECT id FROM rows);
	`, sql, tableName)

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

func createNewTransaction(ctx context.Context, tx *sqlx.Tx) (newTxnID uint64, err error) {
	ctx, span := tracer.Start(ctx, "createNewTransaction")
	defer span.End()

	result, err := tx.ExecContext(ctx, createTxn)
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

	driver, err := migrations.NewMysqlDriver(mds.url)
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
	}

	return common.RevisionFromTransaction(revision), nil
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
	query, args, err := getRevision.ToSql()
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

	query, args, err := getRevision.ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var revision uint64
	err = mds.db.QueryRowxContext(datastore.SeparateContextWithTracing(ctx), query, args...).Scan(&revision)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf(errRevision, err)
	}

	return revision, nil
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

	query, args, err := getRevisionRange.Where(sq.GtOrEq{common.ColTimestamp: lowerBound}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	var lower, upper sql.NullInt64
	err = mds.db.QueryRowxContext(
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
