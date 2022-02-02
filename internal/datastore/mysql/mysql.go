package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	errNotImplemented = "spicedb/datastore/mysql: Not Implemented"

	tableNamespace   = "namespace_config"
	tableTransaction = "relation_tuple_transaction"
	tableTuple       = "relation_tuple"

	colNamespace  = "namespace"
	colConfig     = "serialized_config"
	colCreatedTxn = "created_transaction"
	colDeletedTxn = "deleted_transaction"

	errRevision = "unable to find revision: %w"

	createTxn = "INSERT INTO relation_tuple_transaction VALUES()"

	liveDeletedTxnID = uint64(9223372036854775807)
)

var (
	tracer      = otel.Tracer("spicedb/internal/datastore/mysql")
	getRevision = sb.Select("MAX(id)").From(tableTransaction)

	sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)
)

func NewMysqlDatastore(url string) (datastore.Datastore, error) {
	db, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("NewMysqlDatastore: failed to open database: %w", err)
	}

	return &mysqlDatastore{db}, nil
}

type mysqlDatastore struct {
	db *sqlx.DB
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
	err := mds.db.PingContext(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

// OptimizedRevision gets a revision that will likely already be replicated
// and will likely be shared amongst many queries.
func (mds *mysqlDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
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

	return revisionFromTransaction(revision), nil
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

func revisionFromTransaction(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}

func transactionFromRevision(revision datastore.Revision) uint64 {
	return uint64(revision.IntPart())
}

func filterToLivingObjects(original sq.SelectBuilder, revision datastore.Revision) sq.SelectBuilder {
	return original.Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
		Where(sq.Or{
			sq.Eq{colDeletedTxn: liveDeletedTxnID},
			sq.Gt{colDeletedTxn: revision},
		})
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
