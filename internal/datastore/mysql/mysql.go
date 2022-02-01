package mysql

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
)

const (
	errNotImplemented = "spicedb/datastore/mysql: Not Implemented"

	tableNamespace = "namespace_config"
	tableTuple     = "relation_tuple"

	colNamespace  = "namespace"
	colConfig     = "serialized_config"
	colCreatedTxn = "created_transaction"
	colDeletedTxn = "deleted_transaction"

	createTxn = "INSERT INTO relation_tuple_transaction DEFAULT VALUES"

	liveDeletedTxnID = uint64(9223372036854775807)
)

var (
	psql   = sq.StatementBuilder.PlaceholderFormat(sq.Question)
	tracer = otel.Tracer("spicedb/internal/datastore/mysql")
)

func NewMysqlDatastore(url string) (datastore.Datastore, error) {
	db, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("NewMysqlDatastore: failed to open database: %w", err)
	}

	return &mysqlDatastore{conn: db}, nil
}

type mysqlDatastore struct {
	conn *sqlx.DB
}

// Close closes the data store.
func (mds *mysqlDatastore) Close() error {
	return nil
}

// IsReady returns whether the datastore is ready to accept data. Datastores that require
// database schema creation will return false until the migrations have been run to create
// the necessary tables.
func (mds *mysqlDatastore) IsReady(ctx context.Context) (bool, error) {
	err := mds.conn.DB.PingContext(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

// WriteTuples takes a list of existing tuples that must exist, and a list of
// tuple mutations and applies it to the datastore for the specified
// namespace.
func (mds *mysqlDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// DeleteRelationships deletes all Relationships that match the provided
// filter if all preconditions are met.
func (mds *mysqlDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// OptimizedRevision gets a revision that will likely already be replicated
// and will likely be shared amongst many queries.
func (mds *mysqlDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// HeadRevision gets a revision that is guaranteed to be at least as fresh as
// right now.
func (mds *mysqlDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// Watch notifies the caller about all changes to tuples.
//
// All events following afterRevision will be sent to the caller.
func (mds *mysqlDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return nil, nil
}

// QueryTuples reads relationships starting from the resource side.
func (mds *mysqlDatastore) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	options ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	return nil, fmt.Errorf(errNotImplemented)
}

// ReverseQueryTuples reads relationships starting from the subject.
func (mds *mysqlDatastore) ReverseQueryTuples(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	options ...options.ReverseQueryOptionsOption,
) (datastore.TupleIterator, error) {
	return nil, fmt.Errorf(errNotImplemented)
}

// CheckRevision checks the specified revision to make sure it's valid and
// hasn't been garbage collected.
func (mds *mysqlDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return fmt.Errorf(errNotImplemented)
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
