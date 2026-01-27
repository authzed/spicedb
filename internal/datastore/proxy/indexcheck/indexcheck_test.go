package indexcheck

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestIndexCheckingMissingIndex(t *testing.T) {
	ds := fakeDatastore{indexesUsed: []string{"anotherindex"}}
	wrapped := WrapWithIndexCheckingDatastoreProxyIfApplicable(ds)
	require.NotNil(t, wrapped.(*indexcheckingProxy).delegate)

	headRev, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := wrapped.SnapshotReader(headRev)
	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"somedoc"},
		OptionalResourceRelation: "viewer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: "user",
				OptionalSubjectIds:  []string{"tom"},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
			},
		},
	}, options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects))
	require.NoError(t, err)

	for _, err := range it {
		require.ErrorContains(t, err, "expected index(es) [anotherindex] for query shape check-permission-select-direct-subjects not used: EXPLAIN IS FAKE")
	}
}

func TestIndexCheckingFoundIndex(t *testing.T) {
	ds := fakeDatastore{indexesUsed: []string{"testindex"}}
	wrapped := WrapWithIndexCheckingDatastoreProxyIfApplicable(ds)
	require.NotNil(t, wrapped.(*indexcheckingProxy).delegate)

	headRev, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)

	reader := wrapped.SnapshotReader(headRev)
	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"somedoc"},
		OptionalResourceRelation: "viewer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: "user",
				OptionalSubjectIds:  []string{"tom"},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
			},
		},
	}, options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects))
	require.NoError(t, err)

	for _, err := range it {
		require.NoError(t, err)
	}
}

func TestWrapWithIndexCheckingDatastoreProxyIfApplicable(t *testing.T) {
	t.Run("wraps SQL datastore", func(t *testing.T) {
		ds := fakeDatastore{}
		wrapped := WrapWithIndexCheckingDatastoreProxyIfApplicable(ds)
		require.IsType(t, &indexcheckingProxy{}, wrapped)
	})

	t.Run("returns non-SQL datastore as-is", func(t *testing.T) {
		ds := &nonSQLDatastore{}
		wrapped := WrapWithIndexCheckingDatastoreProxyIfApplicable(ds)
		require.Equal(t, ds, wrapped)
	})
}

func TestIndexCheckingProxyMethods(t *testing.T) {
	ds := fakeDatastore{}
	proxy := newIndexCheckingDatastoreProxy(ds).(*indexcheckingProxy)

	t.Run("MetricsID", func(t *testing.T) {
		id, err := proxy.MetricsID()
		require.NoError(t, err)
		require.Equal(t, "fake", id)
	})

	t.Run("OptimizedRevision", func(t *testing.T) {
		rev, err := proxy.OptimizedRevision(t.Context())
		require.NoError(t, err)
		require.Nil(t, rev)
	})

	t.Run("CheckRevision", func(t *testing.T) {
		err := proxy.CheckRevision(t.Context(), datastore.NoRevision)
		require.NoError(t, err)
	})

	t.Run("HeadRevision", func(t *testing.T) {
		rev, err := proxy.HeadRevision(t.Context())
		require.NoError(t, err)
		require.Nil(t, rev)
	})

	t.Run("RevisionFromString", func(t *testing.T) {
		rev, err := proxy.RevisionFromString("test")
		require.NoError(t, err)
		require.Nil(t, rev)
	})

	t.Run("Watch", func(t *testing.T) {
		changes, errs := proxy.Watch(t.Context(), nil, datastore.WatchOptions{})
		require.Nil(t, changes)
		require.Nil(t, errs)
	})

	t.Run("Features", func(t *testing.T) {
		features, err := proxy.Features(t.Context())
		require.NoError(t, err)
		require.Nil(t, features)
	})

	t.Run("OfflineFeatures", func(t *testing.T) {
		features, err := proxy.OfflineFeatures()
		require.Nil(t, features)
		require.NoError(t, err)
	})

	t.Run("Statistics", func(t *testing.T) {
		stats, err := proxy.Statistics(t.Context())
		require.NoError(t, err)
		require.Equal(t, datastore.Stats{}, stats)
	})

	t.Run("Unwrap", func(t *testing.T) {
		unwrapped := proxy.Unwrap()
		require.Equal(t, ds, unwrapped)
	})

	t.Run("ReadyState", func(t *testing.T) {
		state, err := proxy.ReadyState(t.Context())
		require.NoError(t, err)
		require.Equal(t, datastore.ReadyState{}, state)
	})

	t.Run("Close", func(t *testing.T) {
		err := proxy.Close()
		require.NoError(t, err)
	})
}

func TestIndexCheckingReaderMethods(t *testing.T) {
	ds := fakeDatastore{}
	proxy := newIndexCheckingDatastoreProxy(ds)
	reader := proxy.SnapshotReader(nil)

	t.Run("CountRelationships", func(t *testing.T) {
		count, err := reader.CountRelationships(t.Context(), "test")
		require.Error(t, err)
		require.Equal(t, -1, count)
	})

	t.Run("LookupCounters", func(t *testing.T) {
		counters, err := reader.LookupCounters(t.Context())
		require.Error(t, err)
		require.Nil(t, counters)
	})

	t.Run("ReadCaveatByName", func(t *testing.T) {
		caveat, rev, err := reader.ReadCaveatByName(t.Context(), "test")
		require.Error(t, err)
		require.Nil(t, caveat)
		require.Nil(t, rev)
	})

	t.Run("LookupCaveatsWithNames", func(t *testing.T) {
		caveats, err := reader.LookupCaveatsWithNames(t.Context(), []string{"test"})
		require.Error(t, err)
		require.Nil(t, caveats)
	})

	t.Run("ListAllCaveats", func(t *testing.T) {
		caveats, err := reader.ListAllCaveats(t.Context())
		require.Error(t, err)
		require.Nil(t, caveats)
	})

	t.Run("ListAllNamespaces", func(t *testing.T) {
		namespaces, err := reader.ListAllNamespaces(t.Context())
		require.NoError(t, err)
		require.Nil(t, namespaces)
	})

	t.Run("LookupNamespacesWithNames", func(t *testing.T) {
		namespaces, err := reader.LookupNamespacesWithNames(t.Context(), []string{"test"})
		require.Error(t, err)
		require.Nil(t, namespaces)
	})

	t.Run("ReadNamespaceByName", func(t *testing.T) {
		ns, rev, err := reader.ReadNamespaceByName(t.Context(), "test")
		require.Error(t, err)
		require.Nil(t, ns)
		require.Nil(t, rev)
	})

	t.Run("successful reverse query", func(t *testing.T) {
		it, err := reader.ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{
			SubjectType:        "user",
			OptionalSubjectIds: []string{"tom"},
		}, options.WithQueryShapeForReverse(queryshape.MatchingResourcesForSubject),
			options.WithResRelation(&options.ResourceRelation{
				Namespace: "document",
				Relation:  "viewer",
			}))
		require.NoError(t, err)

		for _, err := range it {
			require.NoError(t, err)
		}
	})
}

func TestIndexCheckingRWT(t *testing.T) {
	ds := fakeDatastore{}
	proxy := newIndexCheckingDatastoreProxy(ds)

	_, err := proxy.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		indexRWT := rwt.(*indexcheckingRWT)

		t.Run("RegisterCounter", func(t *testing.T) {
			err := indexRWT.RegisterCounter(ctx, "test", &core.RelationshipFilter{})
			require.NoError(t, err)
		})

		t.Run("UnregisterCounter", func(t *testing.T) {
			err := indexRWT.UnregisterCounter(ctx, "test")
			require.NoError(t, err)
		})

		t.Run("StoreCounterValue", func(t *testing.T) {
			err := indexRWT.StoreCounterValue(ctx, "test", 10, nil)
			require.NoError(t, err)
		})

		t.Run("WriteCaveats", func(t *testing.T) {
			err := indexRWT.WriteCaveats(ctx, []*core.CaveatDefinition{})
			require.NoError(t, err)
		})

		t.Run("DeleteCaveats", func(t *testing.T) {
			err := indexRWT.DeleteCaveats(ctx, []string{"test"})
			require.NoError(t, err)
		})

		t.Run("WriteRelationships", func(t *testing.T) {
			err := indexRWT.WriteRelationships(ctx, []tuple.RelationshipUpdate{})
			require.NoError(t, err)
		})

		t.Run("WriteNamespaces", func(t *testing.T) {
			err := indexRWT.WriteNamespaces(ctx, &core.NamespaceDefinition{})
			require.NoError(t, err)
		})

		t.Run("DeleteNamespaces", func(t *testing.T) {
			err := indexRWT.DeleteNamespaces(ctx, []string{"test"}, datastore.DeleteNamespacesAndRelationships)
			require.NoError(t, err)
		})

		t.Run("DeleteRelationships", func(t *testing.T) {
			count, partial, err := indexRWT.DeleteRelationships(ctx, &v1.RelationshipFilter{})
			require.NoError(t, err)
			require.Equal(t, uint64(0), count)
			require.False(t, partial)
		})

		t.Run("BulkLoad", func(t *testing.T) {
			count, err := indexRWT.BulkLoad(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(0), count)
		})

		return nil
	})
	require.NoError(t, err)
}

func TestMustEnsureIndexes(t *testing.T) {
	t.Run("parse explain error", func(t *testing.T) {
		ds := &fakeDatastoreWithParseError{}
		reader := &indexcheckingReader{parent: ds}

		err := reader.mustEnsureIndexes(t.Context(), "sql", nil, queryshape.CheckPermissionSelectDirectSubjects, "explain", options.SQLIndexInformation{
			ExpectedIndexNames: []string{"testindex"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse explain output")
	})

	t.Run("no expected indexes", func(t *testing.T) {
		ds := fakeDatastore{}
		reader := &indexcheckingReader{parent: ds}

		err := reader.mustEnsureIndexes(t.Context(), "sql", nil, queryshape.CheckPermissionSelectDirectSubjects, "explain", options.SQLIndexInformation{
			ExpectedIndexNames: []string{},
		})
		require.NoError(t, err)
	})

	t.Run("no indexes used", func(t *testing.T) {
		ds := &fakeDatastoreNoIndexes{}
		reader := &indexcheckingReader{parent: ds}

		err := reader.mustEnsureIndexes(t.Context(), "sql", nil, queryshape.CheckPermissionSelectDirectSubjects, "explain", options.SQLIndexInformation{
			ExpectedIndexNames: []string{"testindex"},
		})
		require.NoError(t, err)
	})
}

type nonSQLDatastore struct {
	datastore.Datastore
}

type fakeDatastoreWithParseError struct {
	fakeDatastore
}

func (f *fakeDatastoreWithParseError) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	return datastore.ParsedExplain{}, errors.New("parse error")
}

type fakeDatastoreNoIndexes struct {
	fakeDatastore
}

func (f *fakeDatastoreNoIndexes) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	return datastore.ParsedExplain{
		IndexesUsed: []string{},
	}, nil
}
