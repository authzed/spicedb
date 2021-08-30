package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	errUnableToWriteConfig  = "unable to write namespace config: %w"
	errUnableToReadConfig   = "unable to read namespace config: %w"
	errUnableToDeleteConfig = "unable to delete namespace config: %w"
)

var (
	writeNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
		colCreatedTxn,
	)

	readNamespace = psql.Select(colConfig, colCreatedTxn).
			From(tableNamespace).
			Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	deleteNamespace = psql.Update(tableNamespace).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	deleteNamespaceTuples = psql.Update(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
)

func (pgd *pgDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "WriteNamespace")
	defer span.End()

	span.SetAttributes(common.NamespaceNameKey.String(newConfig.Name))

	serialized, err := proto.Marshal(newConfig)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Serialized namespace config")

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}
	defer tx.Rollback(ctx)
	span.AddEvent("DB transaction established")

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Model transaction created")

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedTxn, newTxnID).
		Where(sq.Eq{colNamespace: newConfig.Name, colDeletedTxn: liveDeletedTxnID}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = tx.Exec(datastore.SeparateContextWithTracing(ctx), delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	sql, args, err := writeNamespace.Values(newConfig.Name, serialized, newTxnID).ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config written")

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config committed")

	return revisionFromTransaction(newTxnID), nil
}

func (pgd *pgDatastore) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer tx.Rollback(ctx)

	loaded, version, err := loadNamespace(ctx, nsName, tx)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
}

func (pgd *pgDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	defer tx.Rollback(ctx)

	_, version, err := loadNamespace(ctx, nsName, tx)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return datastore.NoRevision, err
	case err == nil:
		break
	default:
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedTxn, newTxnID).
		Where(sq.Eq{colNamespace: nsName, colCreatedTxn: version}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := deleteNamespaceTuples.
		Set(colDeletedTxn, newTxnID).
		Where(sq.Eq{colNamespace: nsName}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = tx.Exec(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return version, nil
}

func loadNamespace(ctx context.Context, namespace string, tx pgx.Tx) (*v0.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	sql, args, err := readNamespace.Where(sq.Eq{colNamespace: namespace}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	var config []byte
	var version datastore.Revision
	err = tx.QueryRow(ctx, sql, args...).Scan(&config, &version)
	if err != nil {
		if err == pgx.ErrNoRows {
			err = datastore.NewNamespaceNotFoundErr(namespace)
		}
		return nil, datastore.NoRevision, err
	}

	loaded := &v0.NamespaceDefinition{}
	err = proto.Unmarshal(config, loaded)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	return loaded, version, nil
}

func (pgd *pgDatastore) IsEmpty(ctx context.Context) (bool, error) {
	ctx, span := tracer.Start(ctx, "IsEmpty")

	defer span.End()
	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	nsQuery := psql.Select("1").
		Prefix("NOT EXISTS (").
		From(tableNamespace).
		Limit(1).
		Suffix(")")
	tupleQuery := psql.Select("1").
		Prefix("NOT EXISTS (").
		From(tableTuple).
		Limit(1).
		Suffix(")")

	sql, args, err := psql.Select("1").
		Where(
			sq.And{
				nsQuery,
				tupleQuery,
			}).
		ToSql()
	if err != nil {
		return false, err
	}

	var isEmpty int
	err = tx.QueryRow(ctx, sql, args...).Scan(&isEmpty)
	if err != nil {
		return false, nil
	}

	return isEmpty == 1, nil
}
