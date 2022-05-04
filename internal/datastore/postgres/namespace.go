package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	errUnableToWriteConfig    = "unable to write namespace config: %w"
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToDeleteConfig   = "unable to delete namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

var (
	writeNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
		colCreatedTxn,
	)

	readNamespace   = psql.Select(colConfig, colCreatedTxn).From(tableNamespace)
	deleteNamespace = psql.Update(tableNamespace).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	deleteNamespaceTuples = psql.Update(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
)

func (pgd *pgDatastore) WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "WriteNamespace")
	defer span.End()

	span.SetAttributes(common.ObjNamespaceNameKey.String(newConfig.Name))

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

func (pgd *pgDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer tx.Rollback(ctx)

	loaded, version, err := loadNamespace(ctx, nsName, tx, filterToLivingObjects(readNamespace, revision))
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

	baseQuery := readNamespace.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
	_, createdAt, err := loadNamespace(ctx, nsName, tx, baseQuery)
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
		Where(sq.Eq{colNamespace: nsName, colCreatedTxn: createdAt}).
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

	return revisionFromTransaction(newTxnID), nil
}

func loadNamespace(ctx context.Context, namespace string, tx pgx.Tx, baseQuery sq.SelectBuilder) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	sql, args, err := baseQuery.Where(sq.Eq{colNamespace: namespace}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	var config []byte
	var version datastore.Revision
	err = tx.QueryRow(ctx, sql, args...).Scan(&config, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = datastore.NewNamespaceNotFoundErr(namespace)
		}
		return nil, datastore.NoRevision, err
	}

	loaded := &core.NamespaceDefinition{}
	err = proto.Unmarshal(config, loaded)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	return loaded, version, nil
}

func (pgd *pgDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*core.NamespaceDefinition, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	query := filterToLivingObjects(readNamespace, revision)

	nsDefs, err := loadAllNamespaces(ctx, tx, query)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefs, err
}

func loadAllNamespaces(ctx context.Context, tx pgx.Tx, query sq.SelectBuilder) ([]*core.NamespaceDefinition, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []*core.NamespaceDefinition

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var config []byte
		var version datastore.Revision
		if err := rows.Scan(&config, &version); err != nil {
			return nil, err
		}

		var loaded core.NamespaceDefinition
		if err := proto.Unmarshal(config, &loaded); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, &loaded)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return nsDefs, nil
}
