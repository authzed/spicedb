package datalayer

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewDataLayer creates a new DataLayer wrapping a datastore.Datastore.
func NewDataLayer(ds datastore.Datastore) DataLayer {
	return &defaultDataLayer{ds: ds}
}

// defaultDataLayer wraps a datastore.Datastore and implements DataLayer.
type defaultDataLayer struct {
	ds datastore.Datastore
}

func (d *defaultDataLayer) SnapshotReader(rev datastore.Revision) RevisionedReader {
	return &revisionedReader{reader: d.ds.SnapshotReader(rev)}
}

func (d *defaultDataLayer) ReadWriteTx(ctx context.Context, fn TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	return d.ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return fn(ctx, &readWriteTransaction{rwt: rwt})
	}, opts...)
}

func (d *defaultDataLayer) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return d.ds.OptimizedRevision(ctx)
}

func (d *defaultDataLayer) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return d.ds.HeadRevision(ctx)
}

func (d *defaultDataLayer) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return d.ds.CheckRevision(ctx, revision)
}

func (d *defaultDataLayer) RevisionFromString(serialized string) (datastore.Revision, error) {
	return d.ds.RevisionFromString(serialized)
}

func (d *defaultDataLayer) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return d.ds.Watch(ctx, afterRevision, opts)
}

func (d *defaultDataLayer) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return d.ds.ReadyState(ctx)
}

func (d *defaultDataLayer) Features(ctx context.Context) (*datastore.Features, error) {
	return d.ds.Features(ctx)
}

func (d *defaultDataLayer) OfflineFeatures() (*datastore.Features, error) {
	return d.ds.OfflineFeatures()
}

func (d *defaultDataLayer) Statistics(ctx context.Context) (datastore.Stats, error) {
	return d.ds.Statistics(ctx)
}

func (d *defaultDataLayer) UniqueID(ctx context.Context) (string, error) {
	return d.ds.UniqueID(ctx)
}

func (d *defaultDataLayer) MetricsID() (string, error) {
	return d.ds.MetricsID()
}

func (d *defaultDataLayer) Close() error {
	return d.ds.Close()
}

// revisionedReader wraps a datastore.Reader and implements RevisionedReader.
type revisionedReader struct {
	reader datastore.Reader
}

func (r *revisionedReader) ReadSchema() (SchemaReader, error) {
	return &legacySchemaReaderAdapter{legacyReader: r.reader}, nil
}

func (r *revisionedReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	return r.reader.QueryRelationships(ctx, filter, opts...)
}

func (r *revisionedReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	return r.reader.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (r *revisionedReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return r.reader.CountRelationships(ctx, name)
}

func (r *revisionedReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return r.reader.LookupCounters(ctx)
}

// readWriteTransaction wraps a datastore.ReadWriteTransaction and implements ReadWriteTransaction.
type readWriteTransaction struct {
	rwt datastore.ReadWriteTransaction
}

func (t *readWriteTransaction) ReadSchema() (SchemaReader, error) {
	return &legacySchemaReaderAdapter{legacyReader: t.rwt}, nil
}

func (t *readWriteTransaction) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	return t.rwt.QueryRelationships(ctx, filter, opts...)
}

func (t *readWriteTransaction) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	return t.rwt.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (t *readWriteTransaction) CountRelationships(ctx context.Context, name string) (int, error) {
	return t.rwt.CountRelationships(ctx, name)
}

func (t *readWriteTransaction) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return t.rwt.LookupCounters(ctx)
}

func (t *readWriteTransaction) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	return t.rwt.WriteRelationships(ctx, mutations)
}

func (t *readWriteTransaction) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	return t.rwt.DeleteRelationships(ctx, filter, opts...)
}

func (t *readWriteTransaction) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return t.rwt.BulkLoad(ctx, iter)
}

func (t *readWriteTransaction) WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *caveattypes.TypeSet) error {
	return writeSchemaViaLegacy(ctx, t.rwt, t.rwt, definitions, schemaString, caveatTypeSet)
}

func (t *readWriteTransaction) LegacySchemaWriter() LegacySchemaWriter {
	return &legacySchemaWriterPassthrough{rwt: t.rwt}
}

type legacySchemaWriterPassthrough struct {
	rwt datastore.ReadWriteTransaction
}

func (w *legacySchemaWriterPassthrough) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return w.rwt.LegacyWriteCaveats(ctx, caveats)
}

func (w *legacySchemaWriterPassthrough) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	return w.rwt.LegacyWriteNamespaces(ctx, newConfigs...)
}

func (w *legacySchemaWriterPassthrough) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return w.rwt.LegacyDeleteCaveats(ctx, names)
}

func (w *legacySchemaWriterPassthrough) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return w.rwt.LegacyDeleteNamespaces(ctx, nsNames, delOption)
}

func (t *readWriteTransaction) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	return t.rwt.RegisterCounter(ctx, name, filter)
}

func (t *readWriteTransaction) UnregisterCounter(ctx context.Context, name string) error {
	return t.rwt.UnregisterCounter(ctx, name)
}

func (t *readWriteTransaction) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	return t.rwt.StoreCounterValue(ctx, name, value, computedAtRevision)
}

// datastoreUnwrapper is implemented by DataLayer wrappers to expose the underlying datastore.
type datastoreUnwrapper interface {
	unwrapDatastore() datastore.Datastore
}

func (d *defaultDataLayer) unwrapDatastore() datastore.Datastore { return d.ds }

// UnwrapDatastore extracts the underlying datastore.Datastore from a DataLayer.
// This is for internal use by code that needs raw datastore access (e.g., schema operations).
func UnwrapDatastore(dl DataLayer) datastore.Datastore {
	if u, ok := dl.(datastoreUnwrapper); ok {
		return u.unwrapDatastore()
	}
	return nil
}

// NewReadOnlyDataLayer creates a DataLayer from a ReadOnlyDatastore.
// ReadWriteTx will return a readonly error.
func NewReadOnlyDataLayer(ds datastore.ReadOnlyDatastore) DataLayer {
	return &readOnlyDatastoreAdapter{ds: ds}
}

type readOnlyDatastoreAdapter struct {
	ds datastore.ReadOnlyDatastore
}

func (r *readOnlyDatastoreAdapter) SnapshotReader(rev datastore.Revision) RevisionedReader {
	return &revisionedReader{reader: r.ds.SnapshotReader(rev)}
}

func (r *readOnlyDatastoreAdapter) ReadWriteTx(_ context.Context, _ TxUserFunc, _ ...options.RWTOptionsOption) (datastore.Revision, error) {
	return datastore.NoRevision, datastore.NewReadonlyErr()
}

func (r *readOnlyDatastoreAdapter) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return r.ds.OptimizedRevision(ctx)
}

func (r *readOnlyDatastoreAdapter) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return r.ds.HeadRevision(ctx)
}

func (r *readOnlyDatastoreAdapter) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return r.ds.CheckRevision(ctx, revision)
}

func (r *readOnlyDatastoreAdapter) RevisionFromString(serialized string) (datastore.Revision, error) {
	return r.ds.RevisionFromString(serialized)
}

func (r *readOnlyDatastoreAdapter) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return r.ds.Watch(ctx, afterRevision, opts)
}

func (r *readOnlyDatastoreAdapter) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return r.ds.ReadyState(ctx)
}

func (r *readOnlyDatastoreAdapter) Features(ctx context.Context) (*datastore.Features, error) {
	return r.ds.Features(ctx)
}

func (r *readOnlyDatastoreAdapter) OfflineFeatures() (*datastore.Features, error) {
	return r.ds.OfflineFeatures()
}

func (r *readOnlyDatastoreAdapter) Statistics(ctx context.Context) (datastore.Stats, error) {
	return r.ds.Statistics(ctx)
}

func (r *readOnlyDatastoreAdapter) UniqueID(ctx context.Context) (string, error) {
	return r.ds.UniqueID(ctx)
}

func (r *readOnlyDatastoreAdapter) MetricsID() (string, error) {
	return r.ds.MetricsID()
}

func (r *readOnlyDatastoreAdapter) Close() error {
	return r.ds.Close()
}
