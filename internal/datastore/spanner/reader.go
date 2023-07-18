package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
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
	executor common.QueryExecutor
	txSource txFactory
}

func (sr spannerReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder, err := common.NewSchemaQueryFilterer(schema, queryTuples).FilterWithRelationshipsFilter(filter)
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
	qBuilder, err := common.NewSchemaQueryFilterer(schema, queryTuples).
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
	return func(
		ctx context.Context,
		sql string,
		args []interface{},
	) ([]*core.RelationTuple, error) {
		ctx, span := tracer.Start(ctx, "ExecuteQuery")
		defer span.End()

		iter := txSource().Query(ctx, statementFromSQL(sql, args))

		var tuples []*core.RelationTuple

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

	return ns, revisionFromTimestamp(updated), nil
}

func (sr spannerReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	iter := sr.txSource().Read(
		ctx,
		tableNamespace,
		spanner.AllKeys(),
		[]string{colNamespaceConfig, colNamespaceTS},
	)

	allNamespaces, err := readAllNamespaces(iter)
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

	foundNamespaces, err := readAllNamespaces(iter)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return foundNamespaces, nil
}

func readAllNamespaces(iter *spanner.RowIterator) ([]datastore.RevisionedNamespace, error) {
	var allNamespaces []datastore.RevisionedNamespace
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
			LastWrittenRevision: revisionFromTimestamp(updated),
		})

		return nil
	}); err != nil {
		return nil, err
	}

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
