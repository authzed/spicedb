package indexcheck

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type fakeDatastore struct {
	revision    datastore.Revision
	indexesUsed []string
}

func (f fakeDatastore) MetricsID() (string, error) {
	return "fake", nil
}

func (f fakeDatastore) UniqueID(_ context.Context) (string, error) {
	return "fake", nil
}

func (f fakeDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	return fakeSnapshotReader{
		revision:    revision,
		indexesUsed: f.indexesUsed,
	}
}

func (f fakeDatastore) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, _ ...options.RWTOptionsOption) (datastore.Revision, error) {
	return nil, fn(ctx, &fakeRWT{
		fakeSnapshotReader: fakeSnapshotReader(f),
	})
}

func (f fakeDatastore) OptimizedRevision(_ context.Context) (datastore.Revision, error) {
	return nil, nil
}

func (f fakeDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	return nil, nil
}

func (f fakeDatastore) CheckRevision(_ context.Context, rev datastore.Revision) error {
	if rev != nil && f.revision != nil && rev.GreaterThan(f.revision) {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	return nil
}

func (f fakeDatastore) RevisionFromString(_ string) (datastore.Revision, error) {
	return nil, nil
}

func (f fakeDatastore) Watch(_ context.Context, _ datastore.Revision, _ datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return nil, nil
}

func (f fakeDatastore) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{}, nil
}

func (f fakeDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return nil, nil
}

func (f fakeDatastore) OfflineFeatures() (*datastore.Features, error) {
	return nil, nil
}

func (f fakeDatastore) Statistics(_ context.Context) (datastore.Stats, error) {
	return datastore.Stats{}, nil
}

func (f fakeDatastore) Close() error {
	return nil
}

func (f fakeDatastore) IsStrictReadModeEnabled() bool {
	return true
}

func (f fakeDatastore) BuildExplainQuery(sql string, args []any) (string, []any, error) {
	return "EXPLAIN IS FAKE", nil, nil
}

// ParseExplain parses the output of an EXPLAIN statement.
func (f fakeDatastore) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	return datastore.ParsedExplain{
		IndexesUsed: []string{"testindex"},
	}, nil
}

func (f fakeDatastore) PreExplainStatements() []string {
	return nil
}

type fakeSnapshotReader struct {
	revision    datastore.Revision
	indexesUsed []string
}

func (fsr fakeSnapshotReader) LegacyLookupNamespacesWithNames(_ context.Context, nsNames []string) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	return nil, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LegacyReadNamespaceByName(_ context.Context, nsName string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LegacyLookupCaveatsWithNames(_ context.Context, names []string) ([]datastore.RevisionedDefinition[*corev1.CaveatDefinition], error) {
	return nil, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LegacyReadCaveatByName(_ context.Context, name string) (caveat *corev1.CaveatDefinition, lastWritten datastore.Revision, err error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LegacyListAllCaveats(context.Context) ([]datastore.RevisionedDefinition[*corev1.CaveatDefinition], error) {
	return nil, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LegacyListAllNamespaces(context.Context) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	return nil, nil
}

func (fsr fakeSnapshotReader) QueryRelationships(_ context.Context, _ datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	queryOpts := options.QueryOptions{}
	for _, opt := range opts {
		opt(&queryOpts)
	}
	return fakeIterator(fsr, queryOpts.SQLExplainCallbackForTest), nil
}

func (fsr fakeSnapshotReader) ReverseQueryRelationships(_ context.Context, _ datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	queryOpts := options.ReverseQueryOptions{}
	for _, opt := range opts {
		opt(&queryOpts)
	}
	return fakeIterator(fsr, queryOpts.SQLExplainCallbackForTestForReverse), nil
}

func (fakeSnapshotReader) CountRelationships(ctx context.Context, filter string) (int, error) {
	return -1, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return nil, fmt.Errorf("not implemented")
}

func fakeIterator(fsr fakeSnapshotReader, explainCallback options.SQLExplainCallbackForTest) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		if explainCallback != nil {
			if err := explainCallback(context.Background(), "SOME QUERY", nil, queryshape.CheckPermissionSelectDirectSubjects, "EXPLAIN IS FAKE", options.SQLIndexInformation{
				ExpectedIndexNames: fsr.indexesUsed,
			}); err != nil {
				yield(tuple.Relationship{}, err)
				return
			}
		}

		if !yield(tuple.MustParse("resource:123#viewer@user:tom"), nil) {
			return
		}
		if !yield(tuple.MustParse("resource:456#viewer@user:tom"), nil) {
			return
		}
	}
}

type fakeRWT struct {
	fakeSnapshotReader
}

func (f *fakeRWT) RegisterCounter(ctx context.Context, name string, filter *corev1.RelationshipFilter) error {
	return nil
}

func (f *fakeRWT) UnregisterCounter(ctx context.Context, name string) error {
	return nil
}

func (f *fakeRWT) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	return nil
}

func (f *fakeRWT) LegacyWriteCaveats(ctx context.Context, caveats []*corev1.CaveatDefinition) error {
	return nil
}

func (f *fakeRWT) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return nil
}

func (f *fakeRWT) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	return nil
}

func (f *fakeRWT) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*corev1.NamespaceDefinition) error {
	return nil
}

func (f *fakeRWT) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return nil
}

func (f *fakeRWT) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	return 0, false, nil
}

func (f *fakeRWT) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return 0, nil
}
