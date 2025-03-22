package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type pgReader struct {
	query                pgxcommon.DBFuncQuerier
	executor             common.QueryRelationshipsExecutor
	aliveFilter          queryFilterer
	filterMaximumIDCount uint16
	schema               common.SchemaInformation
}

type queryFilterer func(original sq.SelectBuilder) sq.SelectBuilder

var (
	countRels = psql.Select("COUNT(*)").From(schema.TableTuple)

	readNamespace = psql.
			Select(schema.ColConfig, schema.ColCreatedXid).
			From(schema.TableNamespace)

	readCounters = psql.
			Select(schema.ColCounterName, schema.ColCounterFilter, schema.ColCounterCurrentCount, schema.ColCounterSnapshot).
			From(schema.TableRelationshipCounter)
)

const (
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToReadFilter     = "unable to read relationship filter: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

func (r *pgReader) CountRelationships(ctx context.Context, name string) (int, error) {
	// Ensure the counter is registered.
	counters, err := r.lookupCounters(ctx, name)
	if err != nil {
		return 0, err
	}

	if len(counters) == 0 {
		return 0, datastore.NewCounterNotRegisteredErr(name)
	}

	filter := counters[0].Filter

	relFilter, err := datastore.RelationshipsFilterFromCoreFilter(filter)
	if err != nil {
		return 0, err
	}

	qBuilder, err := common.NewSchemaQueryFiltererWithStartingQuery(r.schema, r.aliveFilter(countRels), r.filterMaximumIDCount).FilterWithRelationshipsFilter(relFilter)
	if err != nil {
		return 0, err
	}

	sql, args, err := qBuilder.UnderlyingQueryBuilder().ToSql()
	if err != nil {
		return 0, fmt.Errorf("unable to count relationships: %w", err)
	}

	var count int
	err = r.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		if !rows.Next() {
			return datastore.NewCounterNotRegisteredErr(name)
		}

		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("unable to read counter: %w", err)
		}
		return rows.Err()
	}, sql, args...)
	if err != nil {
		return 0, err
	}

	return count, nil
}

const noFilterOnCounterName = ""

func (r *pgReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return r.lookupCounters(ctx, noFilterOnCounterName)
}

func (r *pgReader) lookupCounters(ctx context.Context, optionalName string) ([]datastore.RelationshipCounter, error) {
	query := readCounters
	if optionalName != noFilterOnCounterName {
		query = query.Where(sq.Eq{schema.ColCounterName: optionalName})
	}

	sql, args, err := r.aliveFilter(query).ToSql()
	if err != nil {
		return nil, fmt.Errorf("unable to lookup counters: %w", err)
	}

	var counters []datastore.RelationshipCounter
	err = r.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var name string
			var filter []byte
			var snapshot *pgSnapshot
			var currentCount int

			if err := rows.Scan(&name, &filter, &currentCount, &snapshot); err != nil {
				return fmt.Errorf("unable to read counter: %w", err)
			}

			loaded := &core.RelationshipFilter{}
			if err := loaded.UnmarshalVT(filter); err != nil {
				return fmt.Errorf(errUnableToReadFilter, err)
			}

			revision := datastore.NoRevision
			if snapshot != nil {
				revision = postgresRevision{snapshot: *snapshot}
			}

			counters = append(counters, datastore.RelationshipCounter{
				Name:               name,
				Filter:             loaded,
				Count:              currentCount,
				ComputedAtRevision: revision,
			})
		}
		return rows.Err()
	}, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to query counters: %w", err)
	}

	return counters, nil
}

func (r *pgReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFiltererForRelationshipsSelect(r.schema, r.filterMaximumIDCount).
		WithAdditionalFilter(r.aliveFilter).
		FilterWithRelationshipsFilter(filter)
	if err != nil {
		return nil, err
	}

	return r.executor.ExecuteQuery(ctx, qBuilder, opts...)
}

func (r *pgReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFiltererForRelationshipsSelect(r.schema, r.filterMaximumIDCount).
		WithAdditionalFilter(r.aliveFilter).
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

	return r.executor.ExecuteQuery(ctx,
		qBuilder,
		options.WithLimit(queryOpts.LimitForReverse),
		options.WithAfter(queryOpts.AfterForReverse),
		options.WithSort(queryOpts.SortForReverse),
		options.WithQueryShape(queryOpts.QueryShapeForReverse),
		options.WithSQLExplainCallbackForTest(queryOpts.SQLExplainCallbackForTestForReverse),
	)
}

func (r *pgReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	loaded, version, err := r.loadNamespace(ctx, nsName, r.query, r.aliveFilter)
	switch {
	case errors.As(err, &datastore.NamespaceNotFoundError{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
}

func (r *pgReader) loadNamespace(ctx context.Context, namespace string, tx pgxcommon.DBFuncQuerier, filterer queryFilterer) (*core.NamespaceDefinition, postgresRevision, error) {
	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	defs, err := loadAllNamespaces(ctx, tx, func(original sq.SelectBuilder) sq.SelectBuilder {
		return filterer(original).Where(sq.Eq{schema.ColNamespace: namespace})
	})
	if err != nil {
		return nil, postgresRevision{}, err
	}

	if len(defs) < 1 {
		return nil, postgresRevision{}, datastore.NewNamespaceNotFoundErr(namespace)
	}

	return defs[0].Definition, defs[0].LastWrittenRevision.(postgresRevision), nil
}

func (r *pgReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	nsDefsWithRevisions, err := loadAllNamespaces(ctx, r.query, r.aliveFilter)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefsWithRevisions, err
}

func (r *pgReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}

	clause := sq.Or{}
	for _, nsName := range nsNames {
		clause = append(clause, sq.Eq{schema.ColNamespace: nsName})
	}

	nsDefsWithRevisions, err := loadAllNamespaces(ctx, r.query, func(original sq.SelectBuilder) sq.SelectBuilder {
		return r.aliveFilter(original).Where(clause)
	})
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefsWithRevisions, err
}

func loadAllNamespaces(
	ctx context.Context,
	tx pgxcommon.DBFuncQuerier,
	filterer queryFilterer,
) ([]datastore.RevisionedNamespace, error) {
	sql, args, err := filterer(readNamespace).ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []datastore.RevisionedNamespace
	err = tx.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var config []byte
			var version xid8

			if err := rows.Scan(&config, &version); err != nil {
				return err
			}

			loaded := &core.NamespaceDefinition{}
			if err := loaded.UnmarshalVT(config); err != nil {
				return fmt.Errorf(errUnableToReadConfig, err)
			}

			revision := revisionForVersion(version)

			nsDefs = append(nsDefs, datastore.RevisionedNamespace{Definition: loaded, LastWrittenRevision: revision})
		}
		return rows.Err()
	}, sql, args...)
	if err != nil {
		return nil, err
	}

	return nsDefs, nil
}

// revisionForVersion synthesizes a snapshot where the specified version is always visible.
func revisionForVersion(version xid8) postgresRevision {
	return postgresRevision{snapshot: pgSnapshot{
		xmin: version.Uint64 + 1,
		xmax: version.Uint64 + 1,
	}}
}

var _ datastore.Reader = &pgReader{}
