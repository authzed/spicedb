package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/datastore/revisionparsing"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestCheckingReplicatedReaderFallsbackToPrimaryOnCheckRevisionFailure(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("1"), nil}

	replicated, err := NewCheckingReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	// Try at revision 1, which should use the replica.
	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("1"))
	ns, err := reader.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Empty(t, ns)

	require.False(t, reader.(*checkingStableReader).chosePrimaryForTest)

	// Try at revision 2, which should use the primary.
	reader = replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("2"))
	ns, err = reader.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Empty(t, ns)

	require.True(t, reader.(*checkingStableReader).chosePrimaryForTest)
}

func TestCheckingReplicatedReaderFallsbackToPrimaryOnRevisionNotAvailableError(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("1"), nil}

	replicated, err := NewCheckingReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	ns, err := reader.LegacyLookupNamespacesWithNames(t.Context(), []string{"ns1"})
	require.NoError(t, err)
	require.Len(t, ns, 1)
}

func TestReplicatedReaderReturnsExpectedError(t *testing.T) {
	for _, requireCheck := range []bool{true, false} {
		t.Run(fmt.Sprintf("requireCheck=%v", requireCheck), func(t *testing.T) {
			primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
			replica := fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("1"), nil}

			var ds datastore.Datastore
			if requireCheck {
				r, err := NewCheckingReplicatedDatastore(primary, replica)
				require.NoError(t, err)
				ds = r
			} else {
				r, err := NewStrictReplicatedDatastore(primary, replica)
				ds = r
				require.NoError(t, err)
			}

			// Try at revision 1, which should use the replica.
			reader := ds.SnapshotReader(revisionparsing.MustParseRevisionForTest("1"))
			_, _, err := reader.LegacyReadNamespaceByName(t.Context(), "expecterror")
			require.Error(t, err)
			require.ErrorContains(t, err, "raising an expected error")
		})
	}
}

type fakeDatastore struct {
	state       string
	revision    datastore.Revision
	indexesUsed []string
}

func (f fakeDatastore) MetricsID() (string, error) {
	return "fake", nil
}

func (f fakeDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	return fakeSnapshotReader{
		revision:    revision,
		state:       f.state,
		indexesUsed: f.indexesUsed,
	}
}

func (f fakeDatastore) ReadWriteTx(_ context.Context, _ datastore.TxUserFunc, _ ...options.RWTOptionsOption) (datastore.Revision, error) {
	return nil, nil
}

func (f fakeDatastore) OptimizedRevision(_ context.Context) (datastore.Revision, error) {
	return nil, nil
}

func (f fakeDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	return nil, nil
}

func (f fakeDatastore) CheckRevision(_ context.Context, rev datastore.Revision) error {
	if rev.GreaterThan(f.revision) {
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

func (f fakeDatastore) UniqueID(_ context.Context) (string, error) {
	return "fake", nil
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
	state       string
	indexesUsed []string
}

func (fsr fakeSnapshotReader) LegacyLookupNamespacesWithNames(_ context.Context, nsNames []string) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	if fsr.state == "primary" {
		return []datastore.RevisionedDefinition[*corev1.NamespaceDefinition]{
			{
				Definition: &corev1.NamespaceDefinition{
					Name: "ns1",
				},
				LastWrittenRevision: revisionparsing.MustParseRevisionForTest("2"),
			},
		}, nil
	}

	if fsr.revision.GreaterThan(revisionparsing.MustParseRevisionForTest("2")) {
		return nil, common.NewRevisionUnavailableError(fmt.Errorf("revision not available"))
	}

	return nil, fmt.Errorf("not implemented")
}

func (fakeSnapshotReader) LegacyReadNamespaceByName(_ context.Context, nsName string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	if nsName == "expecterror" {
		return nil, nil, fmt.Errorf("raising an expected error")
	}

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
		if fsr.state == "primary" {
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
			return
		}

		if fsr.state == "replica-with-normal-error" {
			if !yield(tuple.MustParse("resource:123#viewer@user:tom"), nil) {
				return
			}
			if !yield(tuple.MustParse("resource:456#viewer@user:tom"), nil) {
				return
			}
			if !yield(tuple.Relationship{}, fmt.Errorf("raising an expected error")) {
				return
			}
			if !yield(tuple.MustParse("resource:789#viewer@user:tom"), nil) {
				return
			}
			return
		}

		if fsr.revision.GreaterThan(revisionparsing.MustParseRevisionForTest("2")) {
			yield(tuple.Relationship{}, common.NewRevisionUnavailableError(fmt.Errorf("revision not available")))
			return
		}

		if !yield(tuple.MustParse("resource:123#viewer@user:tom"), nil) {
			return
		}
		if !yield(tuple.MustParse("resource:456#viewer@user:tom"), nil) {
			return
		}
	}
}
