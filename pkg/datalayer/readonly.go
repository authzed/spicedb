package datalayer

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// NewReadonlyDataLayer wraps a DataLayer so that ReadWriteTx returns an error.
func NewReadonlyDataLayer(dl DataLayer) DataLayer {
	return &readonlyDataLayer{delegate: dl}
}

type readonlyDataLayer struct {
	delegate DataLayer
}

func (r *readonlyDataLayer) SnapshotReader(rev datastore.Revision) RevisionedReader {
	return r.delegate.SnapshotReader(rev)
}

func (r *readonlyDataLayer) ReadWriteTx(_ context.Context, _ TxUserFunc, _ ...options.RWTOptionsOption) (datastore.Revision, error) {
	return datastore.NoRevision, datastore.NewReadonlyErr()
}

func (r *readonlyDataLayer) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return r.delegate.OptimizedRevision(ctx)
}

func (r *readonlyDataLayer) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return r.delegate.HeadRevision(ctx)
}

func (r *readonlyDataLayer) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return r.delegate.CheckRevision(ctx, revision)
}

func (r *readonlyDataLayer) RevisionFromString(serialized string) (datastore.Revision, error) {
	return r.delegate.RevisionFromString(serialized)
}

func (r *readonlyDataLayer) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return r.delegate.Watch(ctx, afterRevision, opts)
}

func (r *readonlyDataLayer) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return r.delegate.ReadyState(ctx)
}

func (r *readonlyDataLayer) Features(ctx context.Context) (*datastore.Features, error) {
	return r.delegate.Features(ctx)
}

func (r *readonlyDataLayer) OfflineFeatures() (*datastore.Features, error) {
	return r.delegate.OfflineFeatures()
}

func (r *readonlyDataLayer) Statistics(ctx context.Context) (datastore.Stats, error) {
	return r.delegate.Statistics(ctx)
}

func (r *readonlyDataLayer) UniqueID(ctx context.Context) (string, error) {
	return r.delegate.UniqueID(ctx)
}

func (r *readonlyDataLayer) MetricsID() (string, error) {
	return r.delegate.MetricsID()
}

func (r *readonlyDataLayer) Close() error {
	return r.delegate.Close()
}

func (r *readonlyDataLayer) unwrapDatastore() datastore.Datastore {
	return UnwrapDatastore(r.delegate)
}
