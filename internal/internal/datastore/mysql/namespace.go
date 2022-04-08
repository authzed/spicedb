package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

func (mds *mysqlDatastore) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	return fmt.Sprintf("%s@%s", namespaceName, revision), nil
}

// WriteNamespace takes a proto namespace definition and persists it,
// returning the version of the namespace that was created.
func (mds *mysqlDatastore) WriteNamespace(ctx context.Context, newNamespace *core.NamespaceDefinition) (datastore.Revision, error) {
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
	defer migrations.LogOnError(ctx, tx.Rollback)
	span.AddEvent("begin DB transaction")

	newTxnID, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteConfig, err)
	}
	span.AddEvent("Model transaction created")

	delSQL, delArgs, err := mds.DeleteNamespaceQuery.
		Set(colDeletedTxn, newTxnID).
		Where(sq.Eq{colNamespace: newNamespace.Name, colDeletedTxn: liveDeletedTxnID}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteConfig, err)
	}

	_, err = tx.ExecContext(datastore.SeparateContextWithTracing(ctx), delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteConfig, err)
	}

	query, args, err := mds.WriteNamespaceQuery.Values(newNamespace.Name, serialized, newTxnID).ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteConfig, err)
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config written")

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config committed")

	return revisionFromTransaction(newTxnID), nil
}

// ReadNamespace reads a namespace definition and version and returns it, and the revision at
// which it was created or last written, if found.
func (mds *mysqlDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(ErrUnableToReadConfig, err)
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

	loaded, version, err := loadNamespace(ctx, nsName, tx, FilterToLivingObjects(mds.ReadNamespaceQuery, revision))
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(ErrUnableToReadConfig, err)
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
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

	baseQuery := mds.ReadNamespaceQuery.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
	_, createdAt, err := loadNamespace(ctx, nsName, tx, baseQuery)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return datastore.NoRevision, err
	case err == nil:
		break
	default:
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	newTxnID, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := mds.DeleteNamespaceQuery.
		Set(colDeletedTxn, newTxnID).
		Where(sq.Eq{colNamespace: nsName, colCreatedTxn: createdAt}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	_, err = tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := mds.DeleteNamespaceTuplesQuery.
		Set(colDeletedTxn, newTxnID).
		Where(sq.Eq{colNamespace: nsName}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	_, err = tx.ExecContext(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteConfig, err)
	}

	return revisionFromTransaction(newTxnID), nil
}

// ListNamespaces lists all namespaces defined.
func (mds *mysqlDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*core.NamespaceDefinition, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

	query, args, err := FilterToLivingObjects(mds.ReadNamespaceQuery, revision).ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []*core.NamespaceDefinition

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer migrations.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var config []byte
		var version datastore.Revision
		if err := rows.Scan(&config, &version); err != nil {
			return nil, fmt.Errorf(ErrUnableToListNamespaces, err)
		}

		var loaded core.NamespaceDefinition
		if err := proto.Unmarshal(config, &loaded); err != nil {
			return nil, fmt.Errorf(ErrUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, &loaded)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return nsDefs, nil
}

func loadNamespace(ctx context.Context, namespace string, tx *sql.Tx, baseQuery sq.SelectBuilder) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	query, args, err := baseQuery.Where(sq.Eq{colNamespace: namespace}).ToSql()
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

	loaded := &core.NamespaceDefinition{}
	err = proto.Unmarshal(config, loaded)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	return loaded, version, nil
}

func loadAllNamespaces(ctx context.Context, db *sql.DB, queryBuilder sq.SelectBuilder) ([]*core.NamespaceDefinition, error) {
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []*core.NamespaceDefinition

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer migrations.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var config []byte
		var version datastore.Revision
		if err := rows.Scan(&config, &version); err != nil {
			return nil, err
		}

		var loaded core.NamespaceDefinition
		if err := proto.Unmarshal(config, &loaded); err != nil {
			return nil, fmt.Errorf(ErrUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, &loaded)
	}

	return nsDefs, nil
}
