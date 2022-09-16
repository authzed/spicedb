package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type txCleanupFunc func() error

type txFactory func(context.Context) (*sql.Tx, txCleanupFunc, error)

type mysqlReader struct {
	*QueryBuilder

	txSource      txFactory
	querySplitter common.TupleQuerySplitter
	filterer      queryFilterer
}

type queryFilterer func(original sq.SelectBuilder) sq.SelectBuilder

const (
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
	errUnableToQueryTuples    = "unable to query tuples: %w"
)

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
var schema = common.SchemaInformation{
	ColNamespace:        colNamespace,
	ColObjectID:         colObjectID,
	ColRelation:         colRelation,
	ColUsersetNamespace: colUsersetNamespace,
	ColUsersetObjectID:  colUsersetObjectID,
	ColUsersetRelation:  colUsersetRelation,
}

func (mr *mysqlReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	qBuilder := common.NewSchemaQueryFilterer(schema, mr.filterer(mr.QueryTuplesQuery)).FilterWithRelationshipsFilter(filter)
	return mr.querySplitter.SplitAndExecuteQuery(ctx, qBuilder, opts...)
}

func (mr *mysqlReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	qBuilder := common.NewSchemaQueryFilterer(schema, mr.filterer(mr.QueryTuplesQuery)).
		FilterWithSubjectsFilter(subjectsFilter)

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return mr.querySplitter.SplitAndExecuteQuery(
		ctx,
		qBuilder,
		options.WithLimit(queryOpts.ReverseLimit),
	)
}

func (mr *mysqlReader) ReadNamespace(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer migrations.LogOnError(ctx, txCleanup)

	loaded, version, err := loadNamespace(ctx, nsName, tx, mr.filterer(mr.ReadNamespaceQuery))
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
}

func loadNamespace(ctx context.Context, namespace string, tx *sql.Tx, baseQuery sq.SelectBuilder) (*core.NamespaceDefinition, datastore.Revision, error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
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
	if err := loaded.UnmarshalVT(config); err != nil {
		return nil, datastore.NoRevision, err
	}

	return loaded, version, nil
}

func (mr *mysqlReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, err
	}
	defer migrations.LogOnError(ctx, txCleanup)

	query := mr.filterer(mr.ReadNamespaceQuery)

	nsDefs, err := loadAllNamespaces(ctx, tx, query)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefs, err
}

func loadAllNamespaces(ctx context.Context, tx *sql.Tx, queryBuilder sq.SelectBuilder) ([]*core.NamespaceDefinition, error) {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	query, args, err := queryBuilder.ToSql()
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
			return nil, err
		}

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(config); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, loaded)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return nsDefs, nil
}

var _ datastore.Reader = &mysqlReader{}
