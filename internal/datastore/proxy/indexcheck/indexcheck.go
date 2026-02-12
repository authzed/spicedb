package indexcheck

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// newIndexCheckingDatastoreProxy returns a datastore proxy that runs EXPLAIN ANALYZE on all
// relationships queries and ensures that the index(es) used within match those defined in the
// schema for the datastore.
func newIndexCheckingDatastoreProxy(d datastore.SQLDatastore) datastore.Datastore {
	return &indexcheckingProxy{delegate: d}
}

// WrapWithIndexCheckingDatastoreProxyIfApplicable wraps the provided datastore with an
// index-checking proxy if the datastore is an SQLDatastore.
func WrapWithIndexCheckingDatastoreProxyIfApplicable(ds datastore.Datastore) datastore.Datastore {
	uds := datastore.UnwrapAs[datastore.SQLDatastore](ds)
	if uds == nil {
		return ds
	}
	return newIndexCheckingDatastoreProxy(uds)
}

type indexcheckingProxy struct{ delegate datastore.SQLDatastore }

func (p *indexcheckingProxy) SnapshotReader(rev datastore.Revision, schemaHash datastore.SchemaHash) datastore.Reader {
	delegateReader := p.delegate.SnapshotReader(rev, schemaHash)
	return &indexcheckingReader{p.delegate, delegateReader}
}

func (p *indexcheckingProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, func(ctx context.Context, delegateRWT datastore.ReadWriteTransaction) error {
		return f(ctx, &indexcheckingRWT{&indexcheckingReader{p.delegate, delegateRWT}, delegateRWT})
	}, opts...)
}

func (p *indexcheckingProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *indexcheckingProxy) UniqueID(ctx context.Context) (string, error) {
	return p.delegate.UniqueID(ctx)
}

func (p *indexcheckingProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, datastore.SchemaHash, error) {
	return p.delegate.OptimizedRevision(ctx)
}

func (p *indexcheckingProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return p.delegate.CheckRevision(ctx, revision)
}

func (p *indexcheckingProxy) HeadRevision(ctx context.Context) (datastore.Revision, datastore.SchemaHash, error) {
	return p.delegate.HeadRevision(ctx)
}

func (p *indexcheckingProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *indexcheckingProxy) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, options)
}

func (p *indexcheckingProxy) Features(ctx context.Context) (*datastore.Features, error) {
	return p.delegate.Features(ctx)
}

func (p *indexcheckingProxy) OfflineFeatures() (*datastore.Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *indexcheckingProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	return p.delegate.Statistics(ctx)
}

func (p *indexcheckingProxy) Unwrap() datastore.Datastore {
	return p.delegate
}

func (p *indexcheckingProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return p.delegate.ReadyState(ctx)
}

func (p *indexcheckingProxy) Close() error { return p.delegate.Close() }

// SchemaHashReaderForTesting returns a test-only interface for reading the schema hash.
// This delegates to the underlying datastore if it supports the test interface.
func (p *indexcheckingProxy) SchemaHashReaderForTesting() interface {
	ReadSchemaHash(ctx context.Context) (string, error)
} {
	type schemaHashReaderProvider interface {
		SchemaHashReaderForTesting() interface {
			ReadSchemaHash(ctx context.Context) (string, error)
		}
	}

	if provider, ok := p.delegate.(schemaHashReaderProvider); ok {
		return provider.SchemaHashReaderForTesting()
	}
	return nil
}

// SchemaModeForTesting returns the current schema mode for testing purposes.
// This delegates to the underlying datastore if it supports the test interface.
func (p *indexcheckingProxy) SchemaModeForTesting() (options.SchemaMode, error) {
	type schemaModeProvider interface {
		SchemaModeForTesting() (options.SchemaMode, error)
	}

	if provider, ok := p.delegate.(schemaModeProvider); ok {
		return provider.SchemaModeForTesting()
	}
	return options.SchemaModeReadLegacyWriteLegacy, errors.New("delegate datastore does not implement SchemaModeForTesting()")
}

type indexcheckingReader struct {
	parent   datastore.SQLDatastore
	delegate datastore.Reader
}

func (r *indexcheckingReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return r.delegate.CountRelationships(ctx, name)
}

func (r *indexcheckingReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return r.delegate.LookupCounters(ctx)
}

func (r *indexcheckingReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return r.delegate.LegacyReadCaveatByName(ctx, name)
}

func (r *indexcheckingReader) LegacyLookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	return r.delegate.LegacyLookupCaveatsWithNames(ctx, caveatNames)
}

func (r *indexcheckingReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return r.delegate.LegacyListAllCaveats(ctx)
}

func (r *indexcheckingReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return r.delegate.LegacyListAllNamespaces(ctx)
}

func (r *indexcheckingReader) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	return r.delegate.LegacyLookupNamespacesWithNames(ctx, nsNames)
}

func (r *indexcheckingReader) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	return r.delegate.LegacyReadNamespaceByName(ctx, nsName)
}

func (r *indexcheckingReader) mustEnsureIndexes(ctx context.Context, sql string, args []any, shape queryshape.Shape, explain string, expectedIndexes options.SQLIndexInformation) error {
	// If no indexes are expected, there is nothing to check.
	if len(expectedIndexes.ExpectedIndexNames) == 0 {
		return nil
	}

	parsed, err := r.parent.ParseExplain(explain)
	if err != nil {
		return fmt.Errorf("failed to parse explain output: %w", err)
	}

	// If an index is not used (perhaps because the data is too small), the query is still valid.
	if len(parsed.IndexesUsed) == 0 {
		return nil
	}

	// Otherwise, ensure the index used is one of the expected indexes.
	indexesUsed := mapz.NewSet(parsed.IndexesUsed...)
	indexesExpected := mapz.NewSet(expectedIndexes.ExpectedIndexNames...)
	if indexesExpected.Intersect(indexesUsed).IsEmpty() {
		return fmt.Errorf("expected index(es) %v for query shape %v not used: %s", expectedIndexes.ExpectedIndexNames, shape, explain)
	}

	return nil
}

func (r *indexcheckingReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	if err := validateQueryShape(queryOpts.QueryShape, filter); err != nil {
		return nil, err
	}

	opts = append(opts, options.WithSQLExplainCallbackForTest(r.mustEnsureIndexes))
	return r.delegate.QueryRelationships(ctx, filter, opts...)
}

func (r *indexcheckingReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)
	if err := validateReverseQueryShape(queryOpts.QueryShapeForReverse, subjectsFilter, queryOpts); err != nil {
		return nil, err
	}

	opts = append(opts, options.WithSQLExplainCallbackForTestForReverse(r.mustEnsureIndexes))
	return r.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

// SchemaReader returns a reference to the wrapped reader, since this
// proxy does not interact with schema methods.
func (r *indexcheckingReader) SchemaReader() (datastore.SchemaReader, error) {
	return r.delegate.SchemaReader()
}

func (r *indexcheckingReader) ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error) {
	singleStoreReader, ok := r.delegate.(datastore.SingleStoreSchemaReader)
	if !ok {
		return nil, errors.New("delegate reader does not implement SingleStoreSchemaReader")
	}
	return singleStoreReader.ReadStoredSchema(ctx)
}

type indexcheckingRWT struct {
	*indexcheckingReader
	delegate datastore.ReadWriteTransaction
}

func (rwt *indexcheckingRWT) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	return rwt.delegate.RegisterCounter(ctx, name, filter)
}

func (rwt *indexcheckingRWT) UnregisterCounter(ctx context.Context, name string) error {
	return rwt.delegate.UnregisterCounter(ctx, name)
}

func (rwt *indexcheckingRWT) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	return rwt.delegate.StoreCounterValue(ctx, name, value, computedAtRevision)
}

func (rwt *indexcheckingRWT) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return rwt.delegate.LegacyWriteCaveats(ctx, caveats)
}

func (rwt *indexcheckingRWT) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return rwt.delegate.LegacyDeleteCaveats(ctx, names)
}

func (rwt *indexcheckingRWT) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	return rwt.delegate.WriteRelationships(ctx, mutations)
}

func (rwt *indexcheckingRWT) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	return rwt.delegate.LegacyWriteNamespaces(ctx, newConfigs...)
}

func (rwt *indexcheckingRWT) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return rwt.delegate.LegacyDeleteNamespaces(ctx, nsNames, delOption)
}

func (rwt *indexcheckingRWT) SchemaWriter() (datastore.SchemaWriter, error) {
	return rwt.delegate.SchemaWriter()
}

func (rwt *indexcheckingRWT) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	singleStoreWriter, ok := rwt.delegate.(datastore.SingleStoreSchemaWriter)
	if !ok {
		return errors.New("delegate transaction does not implement SingleStoreSchemaWriter")
	}
	return singleStoreWriter.WriteStoredSchema(ctx, schema)
}

func (rwt *indexcheckingRWT) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	return rwt.delegate.DeleteRelationships(ctx, filter, options...)
}

func (rwt *indexcheckingRWT) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return rwt.delegate.BulkLoad(ctx, iter)
}

var (
	_ datastore.Datastore               = (*indexcheckingProxy)(nil)
	_ datastore.Reader                  = (*indexcheckingReader)(nil)
	_ datastore.LegacySchemaReader      = (*indexcheckingReader)(nil)
	_ datastore.SingleStoreSchemaReader = (*indexcheckingReader)(nil)
	_ datastore.DualSchemaReader        = (*indexcheckingReader)(nil)
	_ datastore.ReadWriteTransaction    = (*indexcheckingRWT)(nil)
	_ datastore.LegacySchemaWriter      = (*indexcheckingRWT)(nil)
	_ datastore.SingleStoreSchemaWriter = (*indexcheckingRWT)(nil)
	_ datastore.DualSchemaWriter        = (*indexcheckingRWT)(nil)
)
