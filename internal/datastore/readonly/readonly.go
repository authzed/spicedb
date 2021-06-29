package readonly

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
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

func (rd roDatastore) WriteTuples(ctx context.Context, preconditions []*v0.RelationTuple, mutations []*v0.RelationTupleUpdate) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) Revision(ctx context.Context) (datastore.Revision, error) {
	return rd.delegate.Revision(ctx)
}

func (rd roDatastore) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	return rd.delegate.SyncRevision(ctx)
}

func (rd roDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return rd.delegate.Watch(ctx, afterRevision)
}

func (rd roDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	return rd.delegate.ReadNamespace(ctx, nsName)
}

func (rd roDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) QueryTuples(namespace string, revision datastore.Revision) datastore.TupleQuery {
	return rd.delegate.QueryTuples(namespace, revision)
}

func (rd roDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return rd.delegate.ReverseQueryTuplesFromSubject(subject, revision)
}

func (rd roDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return rd.delegate.ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation, revision)
}

func (rd roDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return rd.delegate.CheckRevision(ctx, revision)
}
