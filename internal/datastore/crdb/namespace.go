package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToWriteConfig  = "unable to write namespace config: %w"
	errUnableToReadConfig   = "unable to read namespace config: %w"
	errUnableToDeleteConfig = "unable to delete namespace config: %w"
)

var (
	queryWriteNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
	)
	upsertNamespaceSuffix = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s", colNamespace, colConfig, colConfig)

	queryReadNamespace = psql.Select(colConfig, colTimestamp).From(tableNamespace)

	queryDeleteNamespace = psql.Delete(tableNamespace)
)

func (cds *crdbDatastore) WriteNamespace(ctx context.Context, newConfig *pb.NamespaceDefinition) (datastore.Revision, error) {
	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}
	defer tx.Rollback(ctx)

	serialized, err := proto.Marshal(newConfig)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	writeSql, writeArgs, err := queryWriteNamespace.
		Values(newConfig.Name, serialized).
		Suffix(upsertNamespaceSuffix).
		ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = tx.Exec(ctx, writeSql, writeArgs...)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	now, err := readCRDBNow(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	return now, nil
}

func (cds *crdbDatastore) ReadNamespace(ctx context.Context, nsName string) (*pb.NamespaceDefinition, datastore.Revision, error) {
	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer tx.Rollback(ctx)

	config, timestamp, err := loadNamespace(ctx, tx, nsName)
	switch err {
	case datastore.ErrNamespaceNotFound:
		return nil, datastore.NoRevision, err
	case nil:
		break
	default:
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return config, revisionFromTimestamp(timestamp), nil
}

func (cds *crdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	defer tx.Rollback(ctx)

	_, timestamp, err := loadNamespace(ctx, tx, nsName)
	switch err {
	case datastore.ErrNamespaceNotFound:
		return datastore.NoRevision, err
	case nil:
		break
	default:
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
	if numDeleted == 0 {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, errors.New("delete conflict"))
	}
	if numDeleted > 1 {
		log.Error().Int64("numDeleted", numDeleted).Msg("call to delete deleted too many namespaces")
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, errors.New("internal error"))
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

func loadNamespace(ctx context.Context, tx pgx.Tx, nsName string) (*pb.NamespaceDefinition, time.Time, error) {
	sql, args, err := queryReadNamespace.Where(sq.Eq{colNamespace: nsName}).ToSql()
	if err != nil {
		return nil, time.Time{}, err
	}

	var config []byte
	var timestamp time.Time
	if err := tx.QueryRow(ctx, sql, args...).Scan(&config, &timestamp); err != nil {
		if err == pgx.ErrNoRows {
			err = datastore.ErrNamespaceNotFound
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
