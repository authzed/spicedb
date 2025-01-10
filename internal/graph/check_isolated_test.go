package graph_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/testfixtures"
)

func TestTraitsForArrowRelation(t *testing.T) {
	tcs := []struct {
		name           string
		schema         string
		namespaceName  string
		relationName   string
		expectedTraits graph.Traits
		expectedError  string
	}{
		{
			name:           "unknown namespace",
			schema:         `definition user {}`,
			namespaceName:  "unknown",
			relationName:   "unknown",
			expectedTraits: graph.Traits{},
			expectedError:  "not found",
		},
		{
			name:           "unknown relation",
			schema:         `definition resource {}`,
			namespaceName:  "resource",
			relationName:   "unknown",
			expectedTraits: graph.Traits{},
			expectedError:  "not found",
		},
		{
			name: "known relation with all optimizations",
			schema: `
			definition folder {}

			definition resource {
				relation folder: folder
			}`,
			namespaceName:  "resource",
			relationName:   "folder",
			expectedTraits: graph.Traits{},
		},
		{
			name: "known relation with caveats",
			schema: `
			definition folder {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation folder: folder with somecaveat
			}`,
			namespaceName: "resource",
			relationName:  "folder",
			expectedTraits: graph.Traits{
				HasCaveats: true,
			},
		},
		{
			name: "known relation with expiration",
			schema: `
			use expiration

			definition folder {}

			definition resource {
				relation folder: folder with expiration
			}`,
			namespaceName: "resource",
			relationName:  "folder",
			expectedTraits: graph.Traits{
				HasExpiration: true,
			},
		},
		{
			name: "known relation with caveats and expiration",
			schema: `
			use expiration

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition folder {}

			definition resource {
				relation folder: folder with somecaveat and expiration
			}`,
			namespaceName: "resource",
			relationName:  "folder",
			expectedTraits: graph.Traits{
				HasCaveats:    true,
				HasExpiration: true,
			},
		},
		{
			name: "different relation with caveats and expiration",
			schema: `
			use expiration

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition folder {}

			definition resource {
				relation folder: folder
				relation folder2: folder with somecaveat and expiration
			}`,
			namespaceName:  "resource",
			relationName:   "folder",
			expectedTraits: graph.Traits{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, tc.schema, nil, require)
			reader := ds.SnapshotReader(revision)

			traits, err := graph.TraitsForArrowRelation(context.Background(), reader, tc.namespaceName, tc.relationName)
			if tc.expectedError != "" {
				require.ErrorContains(err, tc.expectedError)
				return
			}

			require.NoError(err)
			require.Equal(tc.expectedTraits, traits)
		})
	}
}
