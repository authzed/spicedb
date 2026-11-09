package schema

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestCachedSchemaSharedAcrossConcurrentRequests(t *testing.T) {
	stored := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{
			NamespaceDefinitions: map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		}},
	})
	views := make([]*Schema, 32)
	errs := make([]error, len(views))
	var workers sync.WaitGroup

	for i := range views {
		workers.Go(func() { views[i], errs[i] = CachedSchemaFor(stored) })
	}

	workers.Wait()

	for i := range views {
		require.NoError(t, errs[i])
		require.Same(t, views[0], views[i])
	}

	definition, ok := views[0].GetTypeDefinition("user")
	require.True(t, ok)
	require.Same(t, views[0], definition.Parent())

	// A replacement schema owns its own view; deleted definitions must not survive.
	replacement := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{}},
	})
	updated, err := CachedSchemaFor(replacement)
	require.NoError(t, err)
	require.NotSame(t, views[0], updated)
	_, ok = updated.GetTypeDefinition("user")
	require.False(t, ok)
}

func TestCachedSchemaRetainsConversionErrorForItsVersion(t *testing.T) {
	// An unsupported rewrite cannot yield a usable view. Repeated requests must
	// retain the failure rather than publish a partial schema as a successful hit.
	stored := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{
			NamespaceDefinitions: map[string]*core.NamespaceDefinition{
				"document": {
					Name:     "document",
					Relation: []*core.Relation{{Name: "view", UsersetRewrite: &core.UsersetRewrite{}}},
				},
			},
		}},
	})

	for range 2 {
		view, err := CachedSchemaFor(stored)
		require.Error(t, err)
		require.Nil(t, view)
	}
}
