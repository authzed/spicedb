package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

// WriteNamespace takes a proto namespace definition and persists it,
// returning the version of the namespace that was created.
func (mds *mysqlDatastore) WriteNamespace(ctx context.Context, newNamespace *v0.NamespaceDefinition) (datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "WriteNamespace")
	defer span.End()

	span.SetAttributes(common.ObjNamespaceNameKey.String(newNamespace.Name))

	serialized, err := proto.Marshal(newNamespace)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("WriteNamespace: failed to serialize config: %w", err)
	}
	span.AddEvent("Serialized namespace config")

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("WriteNamespace: unable to write config: %w", err)
	}
	defer common.LogOnError(ctx, tx.Rollback)
	span.AddEvent("begin DB transaction")

	newTxnID, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteConfig, err)
	}
	span.AddEvent("Model transaction created")

	delSQL, delArgs, err := mds.builderCache.DeleteNamespace.
		Set(common.ColDeletedTxn, newTxnID).
		Where(sq.Eq{common.ColNamespace: newNamespace.Name, common.ColDeletedTxn: liveDeletedTxnID}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteConfig, err)
	}

	_, err = tx.ExecContext(datastore.SeparateContextWithTracing(ctx), delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteConfig, err)
	}

	query, args, err := mds.builderCache.WriteNamespace.Values(newNamespace.Name, serialized, newTxnID).ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteConfig, err)
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config written")

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config committed")

	return common.RevisionFromTransaction(newTxnID), nil
}

// ReadNamespace reads a namespace definition and version and returns it, and the revision at
// which it was created or last written, if found.
func (mds *mysqlDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (ns *v0.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(common.ErrUnableToReadConfig, err)
	}
	defer common.LogOnError(ctx, tx.Rollback)

	loaded, version, err := loadNamespace(ctx, nsName, tx, common.FilterToLivingObjects(mds.builderCache.ReadNamespace, revision, liveDeletedTxnID))
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(common.ErrUnableToReadConfig, err)
	}
}

// DeleteNamespace deletes a namespace and any associated tuples.
func (mds *mysqlDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "DeleteNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}
	defer common.LogOnError(ctx, tx.Rollback)

	baseQuery := mds.builderCache.ReadNamespace.Where(sq.Eq{common.ColDeletedTxn: liveDeletedTxnID})
	_, createdAt, err := loadNamespace(ctx, nsName, tx, baseQuery)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return datastore.NoRevision, err
	case err == nil:
		break
	default:
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	newTxnID, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := mds.builderCache.DeleteNamespace.
		Set(common.ColDeletedTxn, newTxnID).
		Where(sq.Eq{common.ColNamespace: nsName, common.ColCreatedTxn: createdAt}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	_, err = tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := mds.builderCache.DeleteNamespaceTuples.
		Set(common.ColDeletedTxn, newTxnID).
		Where(sq.Eq{common.ColNamespace: nsName}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	_, err = tx.ExecContext(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteConfig, err)
	}

	return common.RevisionFromTransaction(newTxnID), nil
}

// ListNamespaces lists all namespaces defined.
func (mds *mysqlDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*v0.NamespaceDefinition, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, tx.Rollback)

	query, args, err := common.FilterToLivingObjects(mds.builderCache.ReadNamespace, revision, liveDeletedTxnID).ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []*v0.NamespaceDefinition

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var config []byte
		var version datastore.Revision
		if err := rows.Scan(&config, &version); err != nil {
			return nil, fmt.Errorf(common.ErrUnableToListNamespaces, err)
		}

		var loaded v0.NamespaceDefinition
		if err := proto.Unmarshal(config, &loaded); err != nil {
			return nil, fmt.Errorf(common.ErrUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, &loaded)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return nsDefs, nil
}

func loadNamespace(ctx context.Context, namespace string, tx *sql.Tx, baseQuery sq.SelectBuilder) (*v0.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	query, args, err := baseQuery.Where(sq.Eq{common.ColNamespace: namespace}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	var config []byte
	var version datastore.Revision
	err = tx.QueryRowContext(ctx, query, args...).Scan(&config, &version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
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
