package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

var (
	queryReadNamespace = psql.Select(colConfig, colTimestamp)

	queryTuples = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
	)

	schema = common.NewSchemaInformation(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		common.ExpandedLogicComparison,
	)
)

type crdbReader struct {
	query         pgxcommon.DBFuncQuerier
	executor      common.QueryExecutor
	keyer         overlapKeyer
	overlapKeySet keySet
	fromBuilder   func(query sq.SelectBuilder, fromStr string) sq.SelectBuilder
}

func (cr *crdbReader) ReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	config, timestamp, err := cr.loadNamespace(ctx, cr.query, nsName)
	if err != nil {
		if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
			return nil, datastore.NoRevision, err
		}
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return config, revisionFromTimestamp(timestamp), nil
}

func (cr *crdbReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	nsDefs, err := loadAllNamespaces(ctx, cr.query, cr.fromBuilder)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}
	return nsDefs, nil
}

func (cr *crdbReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}
	nsDefs, err := cr.lookupNamespaces(ctx, cr.query, nsNames)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}
	return nsDefs, nil
}

func (cr *crdbReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	query := cr.fromBuilder(queryTuples, tableTuple)
	qBuilder, err := common.NewSchemaQueryFilterer(schema, query).FilterWithRelationshipsFilter(filter)
	if err != nil {
		return nil, err
	}

	return cr.executor.ExecuteQuery(ctx, qBuilder, opts...)
}

func (cr *crdbReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	query := cr.fromBuilder(queryTuples, tableTuple)
	qBuilder, err := common.NewSchemaQueryFilterer(schema, query).
		FilterWithSubjectsSelectors(subjectsFilter.AsSelector())
	if err != nil {
		return nil, err
	}

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return cr.executor.ExecuteQuery(
		ctx,
		qBuilder,
		options.WithLimit(queryOpts.LimitForReverse),
		options.WithAfter(queryOpts.AfterForReverse),
		options.WithSort(queryOpts.SortForReverse))
}

func (cr crdbReader) loadNamespace(ctx context.Context, tx pgxcommon.DBFuncQuerier, nsName string) (*core.NamespaceDefinition, time.Time, error) {
	query := cr.fromBuilder(queryReadNamespace, tableNamespace).Where(sq.Eq{colNamespace: nsName})

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, time.Time{}, err
	}

	var config []byte
	var timestamp time.Time

	err = tx.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&config, &timestamp)
	}, sql, args...)
	if err != nil {
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

func (cr crdbReader) lookupNamespaces(ctx context.Context, tx pgxcommon.DBFuncQuerier, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	clause := sq.Or{}
	for _, nsName := range nsNames {
		clause = append(clause, sq.Eq{colNamespace: nsName})
	}

	query := cr.fromBuilder(queryReadNamespace, tableNamespace).Where(clause)

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []datastore.RevisionedNamespace

	err = tx.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var config []byte
			var timestamp time.Time
			if err := rows.Scan(&config, &timestamp); err != nil {
				return err
			}

			loaded := &core.NamespaceDefinition{}
			if err := loaded.UnmarshalVT(config); err != nil {
				return fmt.Errorf(errUnableToReadConfig, err)
			}

			nsDefs = append(nsDefs, datastore.RevisionedNamespace{
				Definition:          loaded,
				LastWrittenRevision: revisionFromTimestamp(timestamp),
			})
		}

		if rows.Err() != nil {
			return fmt.Errorf(errUnableToReadConfig, rows.Err())
		}
		return nil
	}, sql, args...)
	if err != nil {
		return nil, err
	}

	return nsDefs, nil
}

func loadAllNamespaces(ctx context.Context, tx pgxcommon.DBFuncQuerier, fromBuilder func(sq.SelectBuilder, string) sq.SelectBuilder) ([]datastore.RevisionedNamespace, error) {
	query := fromBuilder(queryReadNamespace, tableNamespace)

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []datastore.RevisionedNamespace

	err = tx.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var config []byte
			var timestamp time.Time
			if err := rows.Scan(&config, &timestamp); err != nil {
				return err
			}

			loaded := &core.NamespaceDefinition{}
			if err := loaded.UnmarshalVT(config); err != nil {
				return fmt.Errorf(errUnableToReadConfig, err)
			}

			nsDefs = append(nsDefs, datastore.RevisionedNamespace{
				Definition:          loaded,
				LastWrittenRevision: revisionFromTimestamp(timestamp),
			})
		}

		if rows.Err() != nil {
			return fmt.Errorf(errUnableToReadConfig, rows.Err())
		}
		return nil
	}, sql, args...)

	if err != nil {
		return nil, err
	}

	return nsDefs, nil
}

func (cr *crdbReader) addOverlapKey(namespace string) {
	cr.keyer.addKey(cr.overlapKeySet, namespace)
}

var _ datastore.Reader = &crdbReader{}
