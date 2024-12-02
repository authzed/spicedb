package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
	errUnableToReadCounter    = "unable to read relationship counter: %w"
)

var (
	queryReadNamespace = psql.Select(colConfig, colTimestamp)

	countRels = psql.Select("count(*)")

	schema = common.NewSchemaInformation(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colExpiration,
		common.ExpandedLogicComparison,
	)

	queryCounters = psql.Select(
		colCounterName,
		colCounterSerializedFilter,
		colCounterCurrentCount,
		colCounterUpdatedAt,
	)
)

type crdbReader struct {
	query                pgxcommon.DBFuncQuerier
	executor             common.QueryExecutor
	keyer                overlapKeyer
	overlapKeySet        keySet
	fromBuilder          func(query sq.SelectBuilder, fromStr string) sq.SelectBuilder
	filterMaximumIDCount uint16
	tupleTableName       string
	withIntegrity        bool
}

func (cr *crdbReader) CountRelationships(ctx context.Context, name string) (int, error) {
	counters, err := cr.lookupCounters(ctx, name)
	if err != nil {
		return 0, err
	}

	if len(counters) == 0 {
		return 0, datastore.NewCounterNotRegisteredErr(name)
	}

	relFilter, err := datastore.RelationshipsFilterFromCoreFilter(counters[0].Filter)
	if err != nil {
		return 0, err
	}

	query := cr.fromBuilder(countRels, cr.tupleTableName)
	builder, err := common.NewSchemaQueryFilterer(schema, query, cr.filterMaximumIDCount).FilterWithRelationshipsFilter(relFilter)
	if err != nil {
		return 0, err
	}

	sql, args, err := builder.UnderlyingQueryBuilder().ToSql()
	if err != nil {
		return 0, err
	}

	var count int
	err = cr.query.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&count)
	}, sql, args...)
	if err != nil {
		return 0, err
	}

	return count, nil
}

const noFilterOnCounterName = ""

func (cr *crdbReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return cr.lookupCounters(ctx, noFilterOnCounterName)
}

func (cr *crdbReader) lookupCounters(ctx context.Context, optionalFilterName string) ([]datastore.RelationshipCounter, error) {
	query := cr.fromBuilder(queryCounters, tableRelationshipCounter)

	if optionalFilterName != noFilterOnCounterName {
		query = query.Where(sq.Eq{colCounterName: optionalFilterName})
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	var counters []datastore.RelationshipCounter
	err = cr.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var name string
			var serializedFilter []byte
			var currentCount int
			var revisionDecimal *decimal.Decimal
			if err := rows.Scan(&name, &serializedFilter, &currentCount, &revisionDecimal); err != nil {
				return err
			}

			loaded := &core.RelationshipFilter{}
			if err := loaded.UnmarshalVT(serializedFilter); err != nil {
				return fmt.Errorf(errUnableToReadCounter, err)
			}

			revision := datastore.NoRevision
			if revisionDecimal != nil {
				rev, err := revisions.NewForHLC(*revisionDecimal)
				if err != nil {
					return fmt.Errorf(errUnableToReadCounter, err)
				}

				revision = rev
			}

			counters = append(counters, datastore.RelationshipCounter{
				Name:               name,
				Filter:             loaded,
				Count:              currentCount,
				ComputedAtRevision: revision,
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

	return counters, nil
}

func (cr *crdbReader) ReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	config, timestamp, err := cr.loadNamespace(ctx, cr.query, nsName)
	if err != nil {
		if errors.As(err, &datastore.NamespaceNotFoundError{}) {
			return nil, datastore.NoRevision, err
		}
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return config, revisions.NewHLCForTime(timestamp), nil
}

func (cr *crdbReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	nsDefs, err := loadAllNamespaces(ctx, cr.query, cr.fromBuilder)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}
	return nsDefs, nil
}

func (cr *crdbReader) queryTuples() sq.SelectBuilder {
	if cr.withIntegrity {
		return psql.Select(
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
			colCaveatContextName,
			colCaveatContext,
			colExpiration,
			colIntegrityKeyID,
			colIntegrityHash,
			colTimestamp,
		)
	}

	return psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
		colExpiration,
	)
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
	query := cr.fromBuilder(cr.queryTuples(), cr.tupleTableName)
	qBuilder, err := common.NewSchemaQueryFilterer(schema, query, cr.filterMaximumIDCount).FilterWithRelationshipsFilter(filter)
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
	query := cr.fromBuilder(cr.queryTuples(), cr.tupleTableName)
	qBuilder, err := common.NewSchemaQueryFilterer(schema, query, cr.filterMaximumIDCount).
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
				LastWrittenRevision: revisions.NewHLCForTime(timestamp),
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
				LastWrittenRevision: revisions.NewHLCForTime(timestamp),
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
