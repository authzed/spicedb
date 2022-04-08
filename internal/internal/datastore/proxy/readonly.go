package proxy

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
)

var errReadOnly = datastore.NewReadonlyErr()

type roDatastore struct {
	delegate datastore.Datastore
}

// NewReadonlyDatastore creates a proxy which disables write operations to a downstream delegate
// datastore.
func NewReadonlyDatastore(delegate datastore.Datastore) datastore.Datastore {
	return roDatastore{delegate: delegate}
}

func (rd roDatastore) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	return rd.delegate.NamespaceCacheKey(namespaceName, revision)
}

func (rd roDatastore) Close() error {
	return rd.delegate.Close()
}

func (rd roDatastore) IsReady(ctx context.Context) (bool, error) {
	return rd.delegate.IsReady(ctx)
}

func (rd roDatastore) DeleteRelationships(ctx context.Context, _ []*v1.Precondition, _ *v1.RelationshipFilter) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) WriteTuples(ctx context.Context, _ []*v1.Precondition, _ []*v1.RelationshipUpdate) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return rd.delegate.OptimizedRevision(ctx)
}

func (rd roDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return rd.delegate.HeadRevision(ctx)
}

func (rd roDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return rd.delegate.Watch(ctx, afterRevision)
}

func (rd roDatastore) WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*core.NamespaceDefinition, datastore.Revision, error) {
	return rd.delegate.ReadNamespace(ctx, nsName, revision)
}

func (rd roDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	options ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	return rd.delegate.QueryTuples(ctx, filter, revision, options...)
}

func (rd roDatastore) ReverseQueryTuples(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	options ...options.ReverseQueryOptionsOption,
) (datastore.TupleIterator, error) {
	return rd.delegate.ReverseQueryTuples(ctx, subjectFilter, revision, options...)
}

func (rd roDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return rd.delegate.CheckRevision(ctx, revision)
}

func (rd roDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*core.NamespaceDefinition, error) {
	return rd.delegate.ListNamespaces(ctx, revision)
}

func (rd roDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	return rd.delegate.Statistics(ctx)
}
