package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	errNotImplemented = "spicedb/datastore/mysql: Not Implemented"

	errRevision = "unable to find revision: %w"

	createTxn = "INSERT INTO relation_tuple_transaction VALUES()"

	liveDeletedTxnID = uint64(9223372036854775807)

	errUnableToInstantiate = "unable to instantiate datastore: %w"
)

var (
	tracer = otel.Tracer("spicedb/internal/datastore/mysql")

	getRevision      = sb.Select("MAX(id)").From(common.TableTransaction)
	getRevisionRange = sb.Select("MIN(id)", "MAX(id)").From(common.TableTransaction)

	getNow = sb.Select("NOW() as now")

	sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)
)

func NewMysqlDatastore(url string, options ...Option) (datastore.Datastore, error) {
	db, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("NewMysqlDatastore: failed to open database: %w", err)
	}

	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &mysqlDatastore{db, config.revisionFuzzingTimedelta}, nil
}

type mysqlDatastore struct {
	db                       *sqlx.DB
	revisionFuzzingTimedelta time.Duration
}

// Close closes the data store.
func (mds *mysqlDatastore) Close() error {
	return nil
}

func createNewTransaction(ctx context.Context, tx *sqlx.Tx) (newTxnID uint64, err error) {
	ctx, span := tracer.Start(ctx, "computeNewTransaction")
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
	return true, nil
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

// Watch notifies the caller about all changes to tuples.
//
// All events following afterRevision will be sent to the caller.
func (mds *mysqlDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return nil, nil
}

// CheckRevision checks the specified revision to make sure it's valid and
// hasn't been garbage collected.
func (mds *mysqlDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return fmt.Errorf(errNotImplemented)
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
	fmt.Println("got revision", revision)

	return revision, nil
}

func (mds *mysqlDatastore) computeRevisionRange(ctx context.Context, windowInverted time.Duration) (uint64, uint64, error) {
	ctx, span := tracer.Start(ctx, "computeRevisionRange")
	defer span.End()

	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return 0, 0, err
	}

	var now time.Time
	err = mds.db.QueryRowxContext(datastore.SeparateContextWithTracing(ctx), nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return 0, 0, err
	}
	// RelationTupleTransaction is not timezone aware
	// Explicitly use UTC before using as a query arg
	now = now.UTC()

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
