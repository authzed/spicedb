package proxy

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
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

func (rd roDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return rd.delegate.SnapshotReader(rev)
}

func (rd roDatastore) ReadWriteTx(context.Context, datastore.TxUserFunc) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}

func (rd roDatastore) Close() error {
	return rd.delegate.Close()
}

func (rd roDatastore) IsReady(ctx context.Context) (bool, error) {
	return rd.delegate.IsReady(ctx)
}

func (rd roDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return rd.delegate.OptimizedRevision(ctx)
}

func (rd roDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return rd.delegate.HeadRevision(ctx)
}

func (rd roDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return rd.delegate.CheckRevision(ctx, revision)
}

func (rd roDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return rd.delegate.Watch(ctx, afterRevision)
}

func (rd roDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	return rd.delegate.Statistics(ctx)
}
