package datalayer

import (
	"context"
	"errors"
	"sync/atomic"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// maxSchemaHashAttempts is the total number of times ReadWriteTx will attempt a transaction
// (including the first) before surfacing ErrSchemaHashPreconditionFailed to the caller.
const maxSchemaHashAttempts = 4

// storedSchemaCache caches stored schemas by hash.
type storedSchemaCache interface {
	GetOrLoad(ctx context.Context, rev datastore.Revision, schemaHash SchemaHash,
		loader func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error)) (*datastore.ReadOnlyStoredSchema, error)
	Set(schemaHash SchemaHash, schema *datastore.ReadOnlyStoredSchema) error
}

// noopSchemaCache is a storedSchemaCache that always delegates to the loader.
type noopSchemaCache struct{}

func (noopSchemaCache) GetOrLoad(ctx context.Context, _ datastore.Revision, _ SchemaHash,
	loader func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error),
) (*datastore.ReadOnlyStoredSchema, error) {
	return loader(ctx)
}

func (noopSchemaCache) Set(_ SchemaHash, _ *datastore.ReadOnlyStoredSchema) error {
	return nil
}

// DataLayerOption configures a DataLayer.
type DataLayerOption func(*defaultDataLayer)

// WithSchemaMode sets the schema mode for the DataLayer.
func WithSchemaMode(mode SchemaMode) DataLayerOption {
	return func(d *defaultDataLayer) {
		d.schemaMode = mode
	}
}

// WithSchemaCache sets the backing schema cache for the DataLayer.
// When set, ReadStoredSchema calls are cached and WriteStoredSchema updates the cache.
func WithSchemaCache(cache SchemaCache) DataLayerOption {
	return func(d *defaultDataLayer) {
		d.cache = newSchemaHashCache(cache)
	}
}

// NewDataLayer creates a new DataLayer wrapping a datastore.Datastore.
func NewDataLayer(ds datastore.Datastore, opts ...DataLayerOption) DataLayer {
	d := &defaultDataLayer{
		ds:         ds,
		schemaMode: SchemaModeReadLegacyWriteLegacy,
		cache:      noopSchemaCache{},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// defaultDataLayer wraps a datastore.Datastore and implements DataLayer.
type defaultDataLayer struct {
	ds             datastore.Datastore
	schemaMode     SchemaMode
	cache          storedSchemaCache
	lastSchemaHash atomic.Pointer[string] // most recently observed non-bypass schema hash
}

// observeSchemaHash stores h as the most recently observed schema hash.
// Ignored for empty or bypass-sentinel values.
func (d *defaultDataLayer) observeSchemaHash(h SchemaHash) {
	if h != "" && !h.IsBypassSentinel() {
		d.lastSchemaHash.Store(new(string(h)))
	}
}

// loadLastSchemaHash returns the most recently observed schema hash, or "" if none.
func (d *defaultDataLayer) loadLastSchemaHash() SchemaHash {
	if p := d.lastSchemaHash.Load(); p != nil {
		return SchemaHash(*p)
	}
	return ""
}

func (d *defaultDataLayer) SnapshotReader(rev datastore.Revision, schemaHash SchemaHash) RevisionedReader {
	if schemaHash == "" {
		_ = spiceerrors.MustBugf("empty string passed as SchemaHash; use a named sentinel")
	}
	d.observeSchemaHash(schemaHash)
	return &revisionedReader{
		reader:     d.ds.SnapshotReader(rev),
		rev:        rev,
		schemaMode: d.schemaMode,
		schemaHash: schemaHash,
		cache:      d.cache,
	}
}

func (d *defaultDataLayer) ReadWriteTx(ctx context.Context, fn TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	rwtOpts := options.NewRWTOptionsWithOptions(opts...)
	preconditionHash := SchemaHash(rwtOpts.SchemaHashPrecondition)

	// Seed the precondition from the most recently observed hash so ReadSchema
	// inside the transaction can hit the cache without an extra HeadRevision call.
	// Falls back to HeadRevision only when nothing has been observed yet.
	if preconditionHash == "" && d.schemaMode.ReadsFromNew() {
		if h := d.loadLastSchemaHash(); h != "" {
			preconditionHash = h
		} else if result, err := d.ds.HeadRevision(ctx); err == nil && result.SchemaHash != "" {
			preconditionHash = SchemaHash(result.SchemaHash)
			d.observeSchemaHash(preconditionHash)
		}
	}

	var err error
	for range maxSchemaHashAttempts {
		var pendingHash SchemaHash
		var pendingSchema *datastore.ReadOnlyStoredSchema

		// Always pass the current preconditionHash as the last option so that on retries,
		// the refreshed hash overrides whatever the caller originally passed (last option wins).
		dsOpts := make([]options.RWTOptionsOption, len(opts)+1)
		copy(dsOpts, opts)
		dsOpts[len(opts)] = options.WithSchemaHashPrecondition(string(preconditionHash))

		var rev datastore.Revision
		rev, err = d.ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			pendingHash, pendingSchema = "", nil // reset on inner retry
			return fn(ctx, &readWriteTransaction{
				rwt:              rwt,
				schemaMode:       d.schemaMode,
				cache:            d.cache,
				preconditionHash: preconditionHash,
				onSchemaWritten: func(h SchemaHash, s *datastore.ReadOnlyStoredSchema) {
					pendingHash, pendingSchema = h, s
				},
			})
		}, dsOpts...)

		if err == nil {
			if pendingHash != "" {
				_ = d.cache.Set(pendingHash, pendingSchema)
				d.observeSchemaHash(pendingHash)
			}
			return rev, nil
		}

		// When the schema changed since the caller fetched the hash, transparently
		// refresh the hash and retry so the callback runs against the current schema.
		// fn was never called (assertSchemaHash fires before fn), so retrying is safe.
		if !rwtOpts.DisableRetries &&
			errors.Is(err, datastore.ErrSchemaHashPreconditionFailed) &&
			preconditionHash != "" {
			// Use HeadRevision (not OptimizedRevision) to guarantee we see the
			// hash of the write that just invalidated our precondition, even when
			// the datastore quantizes revisions or caches the optimized revision.
			result, fetchErr := d.ds.HeadRevision(ctx)
			if fetchErr == nil && result.SchemaHash != "" {
				preconditionHash = SchemaHash(result.SchemaHash)
				d.observeSchemaHash(preconditionHash)
				continue
			}
		}

		return datastore.NoRevision, err
	}
	return datastore.NoRevision, err
}

func (d *defaultDataLayer) OptimizedRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	rev, _, schemaHash, err := d.ds.OptimizedRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}

	if d.schemaMode.ReadsFromNew() && schemaHash != "" {
		hash := SchemaHash(schemaHash)
		d.observeSchemaHash(hash)
		return rev, hash, nil
	}

	return rev, NoSchemaHashInLegacyMode, nil
}

func (d *defaultDataLayer) HeadRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	result, err := d.ds.HeadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}

	if d.schemaMode.ReadsFromNew() && result.SchemaHash != "" {
		hash := SchemaHash(result.SchemaHash)
		d.observeSchemaHash(hash)
		return result.Revision, hash, nil
	}

	return result.Revision, NoSchemaHashInLegacyMode, nil
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
	reader     datastore.Reader
	rev        datastore.Revision
	schemaMode SchemaMode
	schemaHash SchemaHash
	cache      storedSchemaCache
}

func (r *revisionedReader) ReadSchema(ctx context.Context) (SchemaReader, error) {
	if r.schemaMode.ReadsFromNew() {
		return newStoredSchemaReaderAdapter(ctx, r.reader, r.schemaHash, r.rev, r.cache)
	}
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
	rwt        datastore.ReadWriteTransaction
	schemaMode SchemaMode
	cache      storedSchemaCache

	// onSchemaWritten is called by WriteSchema after a successful datastore write.
	// ReadWriteTx sets this to capture the written schema for post-commit cache update.
	onSchemaWritten func(SchemaHash, *datastore.ReadOnlyStoredSchema)

	// preconditionHash, when non-empty, is the hash the datastore asserted at tx open time
	// (via WithSchemaHashPrecondition). ReadSchema uses it directly for cache lookup.
	preconditionHash SchemaHash
}

func (t *readWriteTransaction) ReadSchema(ctx context.Context) (SchemaReader, error) {
	if t.schemaMode.ReadsFromNew() {
		hash := t.preconditionHash
		if hash == "" {
			hash = NoSchemaHashInTransaction
		}
		return newStoredSchemaReaderAdapter(ctx, t.rwt, hash, datastore.NoRevision, t.cache)
	}
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

func (t *readWriteTransaction) WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *caveattypes.TypeSet) (SchemaHash, error) {
	// Write to legacy storage if mode requires it
	if t.schemaMode.WritesToLegacy() {
		if err := writeSchemaViaLegacy(ctx, t.rwt, t.rwt, definitions); err != nil {
			return "", err
		}
	}

	// Write to unified storage if mode requires it
	if t.schemaMode.WritesToNew() {
		schemaHash, schema, err := WriteSchemaViaStoredSchema(ctx, t.rwt, definitions, schemaString)
		if err != nil {
			return "", err
		}
		if t.onSchemaWritten != nil {
			t.onSchemaWritten(schemaHash, schema)
		}
		return schemaHash, nil
	}

	// Legacy-only storage produces no unified schema hash.
	return NoSchemaHashInLegacyMode, nil
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

func (r *readOnlyDatastoreAdapter) SnapshotReader(rev datastore.Revision, schemaHash SchemaHash) RevisionedReader {
	if schemaHash == "" {
		_ = spiceerrors.MustBugf("empty string passed as SchemaHash; use a named sentinel")
	}
	return &revisionedReader{
		reader:     r.ds.SnapshotReader(rev),
		rev:        rev,
		schemaMode: SchemaModeReadLegacyWriteLegacy,
		schemaHash: schemaHash,
	}
}

func (r *readOnlyDatastoreAdapter) ReadWriteTx(_ context.Context, _ TxUserFunc, _ ...options.RWTOptionsOption) (datastore.Revision, error) {
	return datastore.NoRevision, datastore.NewReadonlyErr()
}

func (r *readOnlyDatastoreAdapter) OptimizedRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	rev, _, _, err := r.ds.OptimizedRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}
	return rev, NoSchemaHashInLegacyMode, nil
}

func (r *readOnlyDatastoreAdapter) HeadRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	result, err := r.ds.HeadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}
	return result.Revision, NoSchemaHashInLegacyMode, nil
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
