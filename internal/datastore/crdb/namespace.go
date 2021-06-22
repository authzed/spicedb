package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToWriteConfig  = "unable to write namespace config: %w"
	errUnableToReadConfig   = "unable to read namespace config: %w"
	errUnableToDeleteConfig = "unable to delete namespace config: %w"
)

type updateIntention bool

var (
	forUpdate updateIntention = true
	readOnly  updateIntention = false
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

func (cds *crdbDatastore) WriteNamespace(ctx context.Context, newConfig *pb.NamespaceDefinition) (datastore.Revision, error) {
	serialized, err := proto.Marshal(newConfig)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	writeSql, writeArgs, err := queryWriteNamespace.
		Values(newConfig.Name, serialized).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	var hlcNow decimal.Decimal
	if err := cds.conn.QueryRow(
		datastore.SeparateContextWithTracing(ctx), writeSql, writeArgs...,
	).Scan(&hlcNow); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	return hlcNow, nil
}

func (cds *crdbDatastore) ReadNamespace(ctx context.Context, nsName string) (*pb.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := cds.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer tx.Rollback(ctx)

	config, timestamp, err := loadNamespace(ctx, tx, nsName, readOnly)
	if err != nil {
		if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
			return nil, datastore.NoRevision, err
		}
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return config, revisionFromTimestamp(timestamp), nil
}

func (cds *crdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	defer tx.Rollback(ctx)

	_, timestamp, err := loadNamespace(ctx, tx, nsName, forUpdate)
	if err != nil {
		if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
			return datastore.NoRevision, err
		}
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := queryDeleteNamespace.
		Where(sq.Eq{colNamespace: nsName, colTimestamp: timestamp}).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deletedNSResult, err := tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	numDeleted := deletedNSResult.RowsAffected()
	if numDeleted != 1 {
		log.Warn().Int64("numDeleted", numDeleted).Msg("deleted wrong number of namespaces")
	}

	deleteTupleSQL, deleteTupleArgs, err := queryDeleteTuples.
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

	return revisionFromTimestamp(timestamp), nil
}

func loadNamespace(ctx context.Context, tx pgx.Tx, nsName string, forUpdate updateIntention) (*pb.NamespaceDefinition, time.Time, error) {
	query := queryReadNamespace.Where(sq.Eq{colNamespace: nsName})

	if forUpdate {
		query.Suffix("FOR UPDATE")
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, time.Time{}, err
	}

	var config []byte
	var timestamp time.Time
	if err := tx.QueryRow(ctx, sql, args...).Scan(&config, &timestamp); err != nil {
		if err == pgx.ErrNoRows {
			err = datastore.NewNamespaceNotFoundErr(nsName)
		}
		return nil, time.Time{}, err
	}

	loaded := &pb.NamespaceDefinition{}
	err = proto.Unmarshal(config, loaded)
	if err != nil {
		return nil, time.Time{}, err
	}

	return loaded, timestamp, nil
}
