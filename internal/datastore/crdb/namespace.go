package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToWriteConfig    = "unable to write namespace config: %w"
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToDeleteConfig   = "unable to delete namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

var (
	upsertNamespaceSuffix = fmt.Sprintf(
		"ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s %s",
		colNamespace,
		colConfig,
		colConfig,
		queryReturningTimestamp,
	)
	queryWriteNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
	).Suffix(upsertNamespaceSuffix)

	queryReadNamespace = psql.Select(colConfig, colTimestamp).From(tableNamespace)

	queryDeleteNamespace = psql.Delete(tableNamespace)
)

func (cds *crdbDatastore) WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (datastore.Revision, error) {
	var hlcNow decimal.Decimal
	if err := cds.execute(ctx, cds.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		keySet := newKeySet()
		cds.AddOverlapKey(keySet, newConfig.Name)

		serialized, err := proto.Marshal(newConfig)
		if err != nil {
			return err
		}
		writeSQL, writeArgs, err := queryWriteNamespace.
			Values(newConfig.Name, serialized).
			ToSql()
		if err != nil {
			return err
		}
		for k := range keySet {
			if _, err := tx.Exec(ctx, queryTouchTransaction, k); err != nil {
				return err
			}
		}
		return tx.QueryRow(
			datastore.SeparateContextWithTracing(ctx), writeSQL, writeArgs...,
		).Scan(&hlcNow)
	}); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	return hlcNow, nil
}

func (cds *crdbDatastore) ReadNamespace(
	ctx context.Context,
	nsName string,
	revision datastore.Revision,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	var config *core.NamespaceDefinition
	var timestamp time.Time
	if err := cds.execute(ctx, cds.conn, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		if err := prepareTransaction(ctx, tx, revision); err != nil {
			return fmt.Errorf(errUnableToReadConfig, err)
		}

		config, timestamp, err = loadNamespace(ctx, tx, nsName)
		if err != nil {
			if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
				return err
			}
			return fmt.Errorf(errUnableToReadConfig, err)
		}
		return nil
	}); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return config, revisionFromTimestamp(timestamp), nil
}

func (cds *crdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	var hlcNow decimal.Decimal
	if err := cds.execute(ctx, cds.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var err error
		_, timestamp, err := loadNamespace(ctx, tx, nsName)
		if err != nil {
			return err
		}
		delSQL, delArgs, err := queryDeleteNamespace.
			Suffix(queryReturningTimestamp).
			Where(sq.Eq{colNamespace: nsName, colTimestamp: timestamp}).
			ToSql()
		if err != nil {
			return err
		}

		serr := tx.QueryRow(ctx, delSQL, delArgs...).Scan(&hlcNow)
		if serr != nil {
			return serr
		}

		deleteTupleSQL, deleteTupleArgs, err := queryDeleteTuples.
			Where(sq.Eq{colNamespace: nsName}).
			ToSql()
		if err != nil {
			return err
		}

		modified, rerr := tx.Exec(ctx, deleteTupleSQL, deleteTupleArgs...)
		if rerr != nil {
			return rerr
		}

		numRowsDeleted := modified.RowsAffected()
		if numRowsDeleted == 0 {
			return nil
		}

		hlcNow, err = updateCounter(ctx, tx, -1*numRowsDeleted)
		return err
	}); err != nil {
		if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
			return datastore.NoRevision, err
		}
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return hlcNow, nil
}

func loadNamespace(ctx context.Context, tx pgx.Tx, nsName string) (*core.NamespaceDefinition, time.Time, error) {
	query := queryReadNamespace.Where(sq.Eq{colNamespace: nsName})

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, time.Time{}, err
	}

	var config []byte
	var timestamp time.Time
	if err := tx.QueryRow(ctx, sql, args...).Scan(&config, &timestamp); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = datastore.NewNamespaceNotFoundErr(nsName)
		}
		return nil, time.Time{}, err
	}

	loaded := &core.NamespaceDefinition{}
	err = proto.Unmarshal(config, loaded)
	if err != nil {
		return nil, time.Time{}, err
	}

	return loaded, timestamp, nil
}

func (cds *crdbDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*core.NamespaceDefinition, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}
	defer tx.Rollback(ctx)

	if err := prepareTransaction(ctx, tx, revision); err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	nsDefs, err := loadAllNamespaces(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefs, nil
}

func loadAllNamespaces(ctx context.Context, tx pgx.Tx) ([]*core.NamespaceDefinition, error) {
	query := queryReadNamespace

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
		var timestamp time.Time
		if err := rows.Scan(&config, &timestamp); err != nil {
			return nil, err
		}

		var loaded core.NamespaceDefinition
		if err := proto.Unmarshal(config, &loaded); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, &loaded)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf(errUnableToReadConfig, err)
	}

	return nsDefs, nil
}
