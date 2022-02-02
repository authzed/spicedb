package mysql

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	sqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

const errNotImplemented = "spicedb/datastore/mysql: Not Implemented"

var (
	sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)

	createTxn = "INSERT INTO relation_tuple_transaction DEFAULT VALUES"

	tracer = otel.Tracer("spicedb/internal/datastore/mysql")
)

func NewMysqlDatastore(url string) (datastore.Datastore, error) {
	// TODO: we're currently using a DSN here, not a URI
	dbConfig, err := sqlDriver.ParseDSN(url)
	if err != nil {
		return nil, fmt.Errorf(common.ErrUnableToInstantiate, err)
	}

	db, err := sqlx.Connect("mysql", dbConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf(common.ErrUnableToInstantiate, err)
	}
	err = sqlDriver.SetLogger(&log.Logger)
	if err != nil {
		return nil, fmt.Errorf("unable to set logging to mysql driver: %w", err)
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

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: failed to get last inserted id: %w", err)
	}

	return uint64(lastInsertId), nil
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

// WriteNamespace takes a proto namespace definition and persists it,
// returning the version of the namespace that was created.
func (mds *mysqlDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// ReadNamespace reads a namespace definition and version and returns it, and the revision at
// which it was created or last written, if found.
func (mds *mysqlDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (ns *v0.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	return nil, datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// DeleteNamespace deletes a namespace and any associated tuples.
func (mds *mysqlDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return datastore.NoRevision, fmt.Errorf(errNotImplemented)
}

// ListNamespaces lists all namespaces defined.
func (mds *mysqlDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*v0.NamespaceDefinition, error) {
	return nil, fmt.Errorf(errNotImplemented)
}

// CheckRevision checks the specified revision to make sure it's valid and
// hasn't been garbage collected.
func (mds *mysqlDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return fmt.Errorf(errNotImplemented)
}
