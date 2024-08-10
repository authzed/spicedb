package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// The underlying Spanner shared read transaction interface is not exposed, so we re-create
// the subsection of it which we need here.
// https://github.com/googleapis/google-cloud-go/blob/a33861fe46be42ae150d6015ad39dae6e35e04e8/spanner/transaction.go#L55
type readTX interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

type txFactory func() readTX

type spannerReader struct {
	executor             common.QueryExecutor
	txSource             txFactory
	filterMaximumIDCount uint16
}

func (sr spannerReader) CountRelationships(ctx context.Context, name string) (int, error) {
	// Ensure the counter exists.
	counters, err := sr.lookupCounters(ctx, name)
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

	builder, err := common.NewSchemaQueryFilterer(schema, countTuples, sr.filterMaximumIDCount).FilterWithRelationshipsFilter(relFilter)
	if err != nil {
		return 0, err
	}

	sql, args, err := builder.UnderlyingQueryBuilder().ToSql()
	if err != nil {
		return 0, err
	}

	var count int64
	if err := sr.txSource().Query(ctx, statementFromSQL(sql, args)).Do(func(r *spanner.Row) error {
		return r.Columns(&count)
	}); err != nil {
		return 0, err
	}

	return int(count), nil
}

const noFilterOnCounterName = ""

func (sr spannerReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return sr.lookupCounters(ctx, noFilterOnCounterName)
}

func (sr spannerReader) lookupCounters(ctx context.Context, optionalFilterName string) ([]datastore.RelationshipCounter, error) {
	key := spanner.AllKeys()
	if optionalFilterName != noFilterOnCounterName {
		key = spanner.Key{optionalFilterName}
	}

	iter := sr.txSource().Read(
		ctx,
		tableRelationshipCounter,
		key,
		[]string{colCounterName, colCounterSerializedFilter, colCounterCurrentCount, colCounterUpdatedAtTimestamp},
	)
	defer iter.Stop()

	var counters []datastore.RelationshipCounter
	if err := iter.Do(func(row *spanner.Row) error {
		var name string
		var serializedFilter []byte
		var currentCount int64
		var updatedAt *time.Time
		if err := row.Columns(&name, &serializedFilter, &currentCount, &updatedAt); err != nil {
			return err
		}

		filter := &core.RelationshipFilter{}
		if err := filter.UnmarshalVT(serializedFilter); err != nil {
			return err
		}

		computedAtRevision := datastore.NoRevision
		if updatedAt != nil {
			computedAtRevision = revisions.NewForTime(*updatedAt)
		}

		counters = append(counters, datastore.RelationshipCounter{
			Name:               name,
			Filter:             filter,
			Count:              int(currentCount),
			ComputedAtRevision: computedAtRevision,
		})

		return nil
	}); err != nil {
		return nil, err
	}

	return counters, nil
}

func (sr spannerReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFilterer(schema, queryTuples, sr.filterMaximumIDCount).FilterWithRelationshipsFilter(filter)
	if err != nil {
		return nil, err
	}

	return sr.executor.ExecuteQuery(ctx, qBuilder, opts...)
}

func (sr spannerReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFilterer(schema, queryTuples, sr.filterMaximumIDCount).
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

	return sr.executor.ExecuteQuery(ctx,
		qBuilder,
		options.WithLimit(queryOpts.LimitForReverse),
		options.WithAfter(queryOpts.AfterForReverse),
		options.WithSort(queryOpts.SortForReverse),
	)
}

func queryExecutor(txSource txFactory) common.ExecuteQueryFunc {
	return func(ctx context.Context, sql string, args []any) ([]*core.RelationTuple, error) {
		span := trace.SpanFromContext(ctx)
		span.AddEvent("Query issued to database")
		iter := txSource().Query(ctx, statementFromSQL(sql, args))
		defer iter.Stop()

		var tuples []*core.RelationTuple

		span.AddEvent("start reading iterator")
		if err := iter.Do(func(row *spanner.Row) error {
			nextTuple := &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{},
				Subject:             &core.ObjectAndRelation{},
			}
			var caveatName spanner.NullString
			var caveatCtx spanner.NullJSON
			err := row.Columns(
				&nextTuple.ResourceAndRelation.Namespace,
				&nextTuple.ResourceAndRelation.ObjectId,
				&nextTuple.ResourceAndRelation.Relation,
				&nextTuple.Subject.Namespace,
				&nextTuple.Subject.ObjectId,
				&nextTuple.Subject.Relation,
				&caveatName,
				&caveatCtx,
			)
			if err != nil {
				return err
			}

			nextTuple.Caveat, err = ContextualizedCaveatFrom(caveatName, caveatCtx)
			if err != nil {
				return err
			}

			tuples = append(tuples, nextTuple)

			return nil
		}); err != nil {
			return nil, err
		}

		span.AddEvent("finished reading iterator", trace.WithAttributes(attribute.Int("tupleCount", len(tuples))))
		span.SetAttributes(attribute.Int("count", len(tuples)))
		return tuples, nil
	}
}

func (sr spannerReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	nsKey := spanner.Key{nsName}
	row, err := sr.txSource().ReadRow(
		ctx,
		tableNamespace,
		nsKey,
		[]string{colNamespaceConfig, colNamespaceTS},
	)
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
		}
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	var serialized []byte
	var updated time.Time
	if err := row.Columns(&serialized, &updated); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	ns := &core.NamespaceDefinition{}
	if err := ns.UnmarshalVT(serialized); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return ns, revisions.NewForTime(updated), nil
}

func (sr spannerReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	iter := sr.txSource().Read(
		ctx,
		tableNamespace,
		spanner.AllKeys(),
		[]string{colNamespaceConfig, colNamespaceTS},
	)
	defer iter.Stop()

	allNamespaces, err := readAllNamespaces(iter, trace.SpanFromContext(ctx))
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return allNamespaces, nil
}

func (sr spannerReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}

	keys := make([]spanner.Key, 0, len(nsNames))
	for _, nsName := range nsNames {
		keys = append(keys, spanner.Key{nsName})
	}

	iter := sr.txSource().Read(
		ctx,
		tableNamespace,
		spanner.KeySetFromKeys(keys...),
		[]string{colNamespaceConfig, colNamespaceTS},
	)
	defer iter.Stop()

	foundNamespaces, err := readAllNamespaces(iter, trace.SpanFromContext(ctx))
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return foundNamespaces, nil
}

func readAllNamespaces(iter *spanner.RowIterator, span trace.Span) ([]datastore.RevisionedNamespace, error) {
	var allNamespaces []datastore.RevisionedNamespace
	span.AddEvent("start reading iterator")
	if err := iter.Do(func(row *spanner.Row) error {
		var serialized []byte
		var updated time.Time
		if err := row.Columns(&serialized, &updated); err != nil {
			return err
		}

		ns := &core.NamespaceDefinition{}
		if err := ns.UnmarshalVT(serialized); err != nil {
			return err
		}

		allNamespaces = append(allNamespaces, datastore.RevisionedNamespace{
			Definition:          ns,
			LastWrittenRevision: revisions.NewForTime(updated),
		})

		return nil
	}); err != nil {
		return nil, err
	}
	span.AddEvent("finished reading iterator", trace.WithAttributes(attribute.Int("namespaceCount", len(allNamespaces))))
	span.SetAttributes(attribute.Int("count", len(allNamespaces)))
	return allNamespaces, nil
}

var queryTuples = sql.Select(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatName,
	colCaveatContext,
).From(tableRelationship)

var countTuples = sql.Select("COUNT(*)").From(tableRelationship)

var queryTuplesForDelete = sql.Select(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
).From(tableRelationship)

var schema = common.NewSchemaInformation(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatName,
	common.ExpandedLogicComparison,
)

var _ datastore.Reader = spannerReader{}
