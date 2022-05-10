package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// The underlying Spanner shared read transaction interface is not exposed, so we re-create
// the subsection of it which we need here.
type readTX interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)

	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator

	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

type txFactory func() readTX

type spannerReader struct {
	querySplitter common.TupleQuerySplitter
	txSource      txFactory
}

func (sr spannerReader) QueryRelationships(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToResourceType(filter.ResourceType)

	if filter.OptionalResourceId != "" {
		qBuilder = qBuilder.FilterToResourceID(filter.OptionalResourceId)
	}

	if filter.OptionalRelation != "" {
		qBuilder = qBuilder.FilterToRelation(filter.OptionalRelation)
	}

	if filter.OptionalSubjectFilter != nil {
		qBuilder = qBuilder.FilterToSubjectFilter(filter.OptionalSubjectFilter)
	}

	return sr.querySplitter.SplitAndExecuteQuery(ctx, qBuilder, opts...)
}

func (sr spannerReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToSubjectFilter(subjectFilter)

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return sr.querySplitter.SplitAndExecuteQuery(ctx,
		qBuilder,
		options.WithLimit(queryOpts.ReverseLimit),
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
				ObjectAndRelation: &core.ObjectAndRelation{},
				User: &core.User{
					UserOneof: &core.User_Userset{
						Userset: &core.ObjectAndRelation{},
					},
				},
			}
			userset := nextTuple.User.GetUserset()
			err := row.Columns(
				&nextTuple.ObjectAndRelation.Namespace,
				&nextTuple.ObjectAndRelation.ObjectId,
				&nextTuple.ObjectAndRelation.Relation,
				&userset.Namespace,
				&userset.ObjectId,
				&userset.Relation,
			)
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

func (sr spannerReader) ReadNamespace(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace")
	defer span.End()

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
	if err := proto.Unmarshal(serialized, ns); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return ns, revisionFromTimestamp(updated), nil
}

func (sr spannerReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	ctx, span := tracer.Start(ctx, "ListNamespaces")
	defer span.End()

	iter := sr.txSource().Read(
		ctx,
		tableNamespace,
		spanner.AllKeys(),
		[]string{colNamespaceConfig},
	)

	allNamespaces, err := readAllNamespaces(iter)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return allNamespaces, nil
}

func readAllNamespaces(iter *spanner.RowIterator) ([]*core.NamespaceDefinition, error) {
	var allNamespaces []*core.NamespaceDefinition
	if err := iter.Do(func(row *spanner.Row) error {
		var serialized []byte
		if err := row.Columns(&serialized); err != nil {
			return err
		}

		ns := &core.NamespaceDefinition{}
		if err := proto.Unmarshal(serialized, ns); err != nil {
			return err
		}

		allNamespaces = append(allNamespaces, ns)

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
).From(tableRelationship)

var schema = common.SchemaInformation{
	ColNamespace:        colNamespace,
	ColObjectID:         colObjectID,
	ColRelation:         colRelation,
	ColUsersetNamespace: colUsersetNamespace,
	ColUsersetObjectID:  colUsersetObjectID,
	ColUsersetRelation:  colUsersetRelation,
}

var _ datastore.Reader = spannerReader{}
