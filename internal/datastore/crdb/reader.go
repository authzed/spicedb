package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

var (
	queryReadNamespace = psql.Select(colConfig, colTimestamp).From(tableNamespace)

	queryTuples = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).From(tableTuple)

	schema = common.SchemaInformation{
		ColNamespace:        colNamespace,
		ColObjectID:         colObjectID,
		ColRelation:         colRelation,
		ColUsersetNamespace: colUsersetNamespace,
		ColUsersetObjectID:  colUsersetObjectID,
		ColUsersetRelation:  colUsersetRelation,
	}
)

type crdbReader struct {
	txSource      pgxcommon.TxFactory
	querySplitter common.TupleQuerySplitter
	keyer         overlapKeyer
	overlapKeySet keySet
	execute       executeTxRetryFunc
}

func (cr *crdbReader) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	var config *core.NamespaceDefinition
	var timestamp time.Time
	if err := cr.execute(ctx, func(ctx context.Context) error {
		tx, txCleanup, err := cr.txSource(ctx)
		if err != nil {
			return err
		}
		defer txCleanup(ctx)

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

	cr.addOverlapKey(config.Name)

	return config, revisionFromTimestamp(timestamp), nil
}

func (cr *crdbReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	var nsDefs []*core.NamespaceDefinition
	if err := cr.execute(ctx, func(ctx context.Context) error {
		tx, txCleanup, err := cr.txSource(ctx)
		if err != nil {
			return err
		}
		defer txCleanup(ctx)

		nsDefs, err = loadAllNamespaces(ctx, tx)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	for _, nsDef := range nsDefs {
		cr.addOverlapKey(nsDef.Name)
	}
	return nsDefs, nil
}

func (cr *crdbReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).FilterWithRelationshipsFilter(filter)

	if err := cr.execute(ctx, func(ctx context.Context) error {
		iter, err = cr.querySplitter.SplitAndExecuteQuery(ctx, qBuilder, opts...)
		return err
	}); err != nil {
		return nil, err
	}

	return iter, nil
}

func (cr *crdbReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterWithSubjectsFilter(subjectsFilter)

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	err = cr.execute(ctx, func(ctx context.Context) error {
		iter, err = cr.querySplitter.SplitAndExecuteQuery(
			ctx,
			qBuilder,
			options.WithLimit(queryOpts.ReverseLimit),
		)
		return err
	})

	return
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
	if err := loaded.UnmarshalVT(config); err != nil {
		return nil, time.Time{}, err
	}

	return loaded, timestamp, nil
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

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(config); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, loaded)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf(errUnableToReadConfig, rows.Err())
	}

	return nsDefs, nil
}

func (cr *crdbReader) addOverlapKey(namespace string) {
	cr.keyer.addKey(cr.overlapKeySet, namespace)
}

var _ datastore.Reader = &crdbReader{}
