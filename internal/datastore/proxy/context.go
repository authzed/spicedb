package proxy

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
)

type ctxDatastore struct{}

// NewContextDatastore creates a proxy which sends datastore operations to the datastore found
// in the context.
func NewContextDatastore() datastore.Datastore {
	return ctxDatastore{}
}

func (cd ctxDatastore) Close() error {
	return nil
}

func (cd ctxDatastore) IsReady(ctx context.Context) (bool, error) {
	return datastoremw.MustFromContext(ctx).IsReady(ctx)
}

func (cd ctxDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).DeleteRelationships(ctx, preconditions, filter)
}

func (cd ctxDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, updates []*v1.RelationshipUpdate) (datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).WriteTuples(ctx, preconditions, updates)
}

func (cd ctxDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).OptimizedRevision(ctx)
}

func (cd ctxDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).HeadRevision(ctx)
}

func (cd ctxDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return datastoremw.MustFromContext(ctx).Watch(ctx, afterRevision)
}

func (cd ctxDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).WriteNamespace(ctx, newConfig)
}

func (cd ctxDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*v0.NamespaceDefinition, datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).ReadNamespace(ctx, nsName, revision)
}

func (cd ctxDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return datastoremw.MustFromContext(ctx).DeleteNamespace(ctx, nsName)
}

func (cd ctxDatastore) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	options ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	return datastoremw.MustFromContext(ctx).QueryTuples(ctx, filter, revision)
}

func (cd ctxDatastore) ReverseQueryTuples(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	options ...options.ReverseQueryOptionsOption,
) (datastore.TupleIterator, error) {
	return datastoremw.MustFromContext(ctx).ReverseQueryTuples(ctx, subjectFilter, revision, options...)
}

func (cd ctxDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return datastoremw.MustFromContext(ctx).CheckRevision(ctx, revision)
}

func (cd ctxDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*v0.NamespaceDefinition, error) {
	return datastoremw.MustFromContext(ctx).ListNamespaces(ctx, revision)
}
