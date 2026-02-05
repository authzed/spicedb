package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	schemaadapter "github.com/authzed/spicedb/internal/datastore/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type txCleanupFunc func() error

type txFactory func(context.Context) (*sql.Tx, txCleanupFunc, error)

type mysqlReader struct {
	*QueryBuilder

	txSource             txFactory
	executor             common.QueryRelationshipsExecutor
	aliveFilter          queryFilterer
	filterMaximumIDCount uint16
	schema               common.SchemaInformation
	schemaMode           dsoptions.SchemaMode
	snapshotRevision     datastore.Revision
	schemaTableName      string
	schemaReaderWriter   *common.SQLSchemaReaderWriter[uint64, revisions.TransactionIDRevision]
}

type queryFilterer func(original sq.SelectBuilder) sq.SelectBuilder

const (
	errUnableToReadConfig        = "unable to read namespace config: %w"
	errUnableToListNamespaces    = "unable to list namespaces: %w"
	errUnableToQueryTuples       = "unable to query tuples: %w"
	errUnableToReadCounters      = "unable to read counters: %w"
	errUnableToReadCounterFilter = "unable to read counter filter: %w"
	errUnableToReadCount         = "unable to read count: %w"
)

func (mr *mysqlReader) CountRelationships(ctx context.Context, name string) (int, error) {
	// Ensure the counter is registered.
	counters, err := mr.lookupCounters(ctx, name)
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

	qBuilder, err := common.NewSchemaQueryFiltererWithStartingQuery(mr.schema, mr.aliveFilter(mr.CountRelsQuery), mr.filterMaximumIDCount).FilterWithRelationshipsFilter(relFilter)
	if err != nil {
		return 0, err
	}

	sql, args, err := qBuilder.UnderlyingQueryBuilder().ToSql()
	if err != nil {
		return 0, fmt.Errorf("unable to count relationships: %w", err)
	}

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return 0, fmt.Errorf(errUnableToReadCount, err)
	}
	defer common.LogOnError(ctx, txCleanup)

	var count int
	rows, err := tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	defer common.LogOnError(ctx, rows.Close)

	if rows.Err() != nil {
		return 0, rows.Err()
	}

	if !rows.Next() {
		if rows.Err() != nil {
			return 0, rows.Err()
		}

		return 0, datastore.NewCounterNotRegisteredErr(name)
	}

	if err := rows.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

const noFilterOnCounterName = ""

func (mr *mysqlReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return mr.lookupCounters(ctx, noFilterOnCounterName)
}

func (mr *mysqlReader) lookupCounters(ctx context.Context, optionalName string) ([]datastore.RelationshipCounter, error) {
	query := mr.aliveFilter(mr.ReadCounterQuery)
	if optionalName != noFilterOnCounterName {
		query = query.Where(sq.Eq{colCounterName: optionalName})
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, fmt.Errorf("unable to lookup counters: %w", err)
	}

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, fmt.Errorf(errUnableToReadCounters, err)
	}
	defer common.LogOnError(ctx, txCleanup)

	rows, err := tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, rows.Close)

	var counters []datastore.RelationshipCounter
	for rows.Next() {
		var name string
		var config []byte
		var currentCount int
		var txID uint64
		if err := rows.Scan(&name, &config, &currentCount, &txID); err != nil {
			return nil, err
		}

		filter := &core.RelationshipFilter{}
		if err := filter.UnmarshalVT(config); err != nil {
			return nil, fmt.Errorf(errUnableToReadCounterFilter, err)
		}

		var rev datastore.Revision = revisions.NewForTransactionID(txID)
		if txID == 0 {
			rev = datastore.NoRevision
		}

		counters = append(counters, datastore.RelationshipCounter{
			Name:               name,
			Filter:             filter,
			Count:              currentCount,
			ComputedAtRevision: rev,
		})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return counters, nil
}

func (mr *mysqlReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...dsoptions.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFiltererForRelationshipsSelect(mr.schema, mr.filterMaximumIDCount).
		WithAdditionalFilter(mr.aliveFilter).
		FilterWithRelationshipsFilter(filter)
	if err != nil {
		return nil, err
	}

	return mr.executor.ExecuteQuery(ctx, qBuilder, opts...)
}

func (mr *mysqlReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...dsoptions.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFiltererForRelationshipsSelect(mr.schema, mr.filterMaximumIDCount).
		WithAdditionalFilter(mr.aliveFilter).
		FilterWithSubjectsSelectors(subjectsFilter.AsSelector())
	if err != nil {
		return nil, err
	}

	queryOpts := dsoptions.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return mr.executor.ExecuteQuery(
		ctx,
		qBuilder,
		dsoptions.WithLimit(queryOpts.LimitForReverse),
		dsoptions.WithAfter(queryOpts.AfterForReverse),
		dsoptions.WithSort(queryOpts.SortForReverse),
		dsoptions.WithSkipCaveats(queryOpts.SkipCaveatsForReverse),
		dsoptions.WithSkipExpiration(queryOpts.SkipExpirationForReverse),
		dsoptions.WithQueryShape(queryOpts.QueryShapeForReverse),
		dsoptions.WithSQLExplainCallbackForTest(queryOpts.SQLExplainCallbackForTestForReverse),
	)
}

func (mr *mysqlReader) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer common.LogOnError(ctx, txCleanup)

	loaded, version, err := loadNamespace(ctx, nsName, tx, mr.aliveFilter(mr.ReadNamespaceQuery))
	switch {
	case errors.As(err, &datastore.NamespaceNotFoundError{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
}

func loadNamespace(ctx context.Context, namespace string, tx *sql.Tx, baseQuery sq.SelectBuilder) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "loadNamespace")
	defer span.End()

	query, args, err := baseQuery.Where(sq.Eq{colNamespace: namespace}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	var config []byte
	var txID uint64
	err = tx.QueryRowContext(ctx, query, args...).Scan(&config, &txID)
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

	return loaded, revisions.NewForTransactionID(txID), nil
}

func (mr *mysqlReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, txCleanup)

	query := mr.aliveFilter(mr.ReadNamespaceQuery)

	nsDefs, err := loadAllNamespaces(ctx, tx, query)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefs, err
}

func (mr *mysqlReader) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, txCleanup)

	clause := sq.Or{}
	for _, nsName := range nsNames {
		clause = append(clause, sq.Eq{colNamespace: nsName})
	}

	query := mr.aliveFilter(mr.ReadNamespaceQuery.Where(clause))

	nsDefs, err := loadAllNamespaces(ctx, tx, query)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return nsDefs, err
}

func loadAllNamespaces(ctx context.Context, tx *sql.Tx, queryBuilder sq.SelectBuilder) ([]datastore.RevisionedNamespace, error) {
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	var nsDefs []datastore.RevisionedNamespace

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var config []byte
		var txID uint64
		if err := rows.Scan(&config, &txID); err != nil {
			return nil, err
		}

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(config); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, datastore.RevisionedNamespace{
			Definition:          loaded,
			LastWrittenRevision: revisions.NewForTransactionID(txID),
		})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return nsDefs, nil
}

// SchemaReader returns a SchemaReader for reading schema information.
func (mr *mysqlReader) SchemaReader() (datastore.SchemaReader, error) {
	// Wrap the reader with an unexported schema reader
	reader := &mysqlSchemaReader{r: mr}
	return schemaadapter.NewSchemaReader(reader, mr.schemaMode, mr.snapshotRevision), nil
}

// mysqlSchemaReader wraps a mysqlReader and implements DualSchemaReader.
// This prevents direct access to schema read methods from the reader.
type mysqlSchemaReader struct {
	r *mysqlReader
}

// ReadStoredSchema implements datastore.SingleStoreSchemaReader with revision-aware reading
func (sr *mysqlSchemaReader) ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error) {
	// Create a revision-aware executor that applies alive filter
	executor := &mysqlRevisionAwareExecutor{
		txSource:    sr.r.txSource,
		aliveFilter: sr.r.aliveFilter,
	}

	// Use the shared schema reader/writer to read the schema
	// Cast snapshotRevision to TransactionIDRevision for cache lookup
	var revPtr *revisions.TransactionIDRevision
	if txRev, ok := sr.r.snapshotRevision.(revisions.TransactionIDRevision); ok {
		revPtr = &txRev
	}
	return sr.r.schemaReaderWriter.ReadSchema(ctx, executor, revPtr)
}

// LegacyReadCaveatByName delegates to the underlying reader
func (sr *mysqlSchemaReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return sr.r.LegacyReadCaveatByName(ctx, name)
}

// LegacyListAllCaveats delegates to the underlying reader
func (sr *mysqlSchemaReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return sr.r.LegacyListAllCaveats(ctx)
}

// LegacyLookupCaveatsWithNames delegates to the underlying reader
func (sr *mysqlSchemaReader) LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	return sr.r.LegacyLookupCaveatsWithNames(ctx, names)
}

// LegacyReadNamespaceByName delegates to the underlying reader
func (sr *mysqlSchemaReader) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	return sr.r.LegacyReadNamespaceByName(ctx, nsName)
}

// LegacyListAllNamespaces delegates to the underlying reader
func (sr *mysqlSchemaReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return sr.r.LegacyListAllNamespaces(ctx)
}

// LegacyLookupNamespacesWithNames delegates to the underlying reader
func (sr *mysqlSchemaReader) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedDefinition[*core.NamespaceDefinition], error) {
	return sr.r.LegacyLookupNamespacesWithNames(ctx, nsNames)
}

var (
	_ datastore.Reader             = &mysqlReader{}
	_ datastore.LegacySchemaReader = &mysqlReader{}
	_ datastore.DualSchemaReader   = &mysqlSchemaReader{}
)
