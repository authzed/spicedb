package proxy

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
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
	return DatastoreFromContext(ctx).IsReady(ctx)
}

func (cd ctxDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	return DatastoreFromContext(ctx).DeleteRelationships(ctx, preconditions, filter)
}

func (cd ctxDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, updates []*v1.RelationshipUpdate) (datastore.Revision, error) {
	return DatastoreFromContext(ctx).WriteTuples(ctx, preconditions, updates)
}

func (cd ctxDatastore) Revision(ctx context.Context) (datastore.Revision, error) {
	return DatastoreFromContext(ctx).Revision(ctx)
}

func (cd ctxDatastore) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	return DatastoreFromContext(ctx).SyncRevision(ctx)
}

func (cd ctxDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return DatastoreFromContext(ctx).Watch(ctx, afterRevision)
}

func (cd ctxDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	return DatastoreFromContext(ctx).WriteNamespace(ctx, newConfig)
}

func (cd ctxDatastore) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	return DatastoreFromContext(ctx).ReadNamespace(ctx, nsName)
}

func (cd ctxDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return DatastoreFromContext(ctx).DeleteNamespace(ctx, nsName)
}

func (cd ctxDatastore) QueryTuples(ctx context.Context, filter datastore.TupleQueryResourceFilter, revision datastore.Revision) datastore.TupleQuery {
	return DatastoreFromContext(ctx).QueryTuples(ctx, filter, revision)
}

func (cd ctxDatastore) ReverseQueryTuplesFromSubjectNamespace(ctx context.Context, subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return DatastoreFromContext(ctx).ReverseQueryTuplesFromSubjectNamespace(ctx, subjectNamespace, revision)
}

func (cd ctxDatastore) ReverseQueryTuplesFromSubject(ctx context.Context, subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return DatastoreFromContext(ctx).ReverseQueryTuplesFromSubject(ctx, subject, revision)
}

func (cd ctxDatastore) ReverseQueryTuplesFromSubjectRelation(ctx context.Context, subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return DatastoreFromContext(ctx).ReverseQueryTuplesFromSubjectRelation(ctx, subjectNamespace, subjectRelation, revision)
}

func (cd ctxDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return DatastoreFromContext(ctx).CheckRevision(ctx, revision)
}

func (cd ctxDatastore) ListNamespaces(ctx context.Context) ([]*v0.NamespaceDefinition, error) {
	return DatastoreFromContext(ctx).ListNamespaces(ctx)
}

// Create a new type to prevent context collisions
type datastoreKey string

var datastoreCtxKey datastoreKey = "injected-datastore"

type dsHandle struct{ ds datastore.Datastore }

// DatastoreFromContext returns the datastore found in the context, or panics if not present.
func DatastoreFromContext(ctx context.Context) datastore.Datastore {
	possibleHandle := ctx.Value(datastoreCtxKey)
	if possibleHandle == nil {
		panic("Datastore not set in context")
	}
	datastore := possibleHandle.(*dsHandle).ds
	if possibleHandle == nil {
		panic("Datastore is nil in context")
	}
	return datastore
}

// SetInContext should be called in a gRPC middleware to set the datastore for the context.
func SetInContext(ctx context.Context, ds datastore.Datastore) {
	possibleHandle := ctx.Value(datastoreCtxKey)
	if possibleHandle == nil {
		return
	}

	handle := possibleHandle.(*dsHandle)
	handle.ds = ds
}

// ContextWithHandle returns a new context with a handle for setting the datastore on a context.
func ContextWithHandle(ctx context.Context) context.Context {
	var handle dsHandle
	return context.WithValue(ctx, datastoreCtxKey, &handle)
}
