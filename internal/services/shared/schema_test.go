package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestApplySchemaChanges(t *testing.T) {
	tcs := []struct {
		name                         string
		startingSchema               string
		relationships                []string
		endingSchema                 string
		expectedAppliedSchemaChanges AppliedSchemaChanges
		expectedError                string
	}{
		{
			name: "various changes",
			startingSchema: `
				definition user {}

				definition document {
					relation viewer: user
					permission view = viewer
				}

				caveat hasFortyTwo(value int) {
				value == 42
				}
			`,
			endingSchema: `
				definition user {}

				definition organization {
					relation member: user
					permission admin = member
				}

				caveat catchTwentyTwo(value int) {
				value == 22
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount:   5,
				NewObjectDefNames:     []string{"organization"},
				RemovedObjectDefNames: []string{"document"},
				NewCaveatDefNames:     []string{"catchTwentyTwo"},
				RemovedCaveatDefNames: []string{"hasFortyTwo"},
			},
		},
		{
			name: "attempt to remove a relation with relationships",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {
					relation viewer: user | group#member | org#admin
				}`,
			relationships: []string{"document:somedoc#viewer@user:alice"},
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {}`,
			expectedError: "cannot delete relation `viewer` in object definition `document`, as a relationship exists under it",
		},
		{
			name: "attempt to remove a relation with indirect relationships",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {
					relation viewer: user | group#member | org#admin
				}`,
			relationships: []string{"document:somedoc#viewer@group:somegroup#member"},
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {}`,
			expectedError: "cannot delete relation `viewer` in object definition `document`, as a relationship exists under it",
		},
		{
			name: "attempt to remove a relation with other indirect relationships",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {
					relation viewer: user | group#member | org#admin
				}`,
			relationships: []string{"document:somedoc#viewer@org:someorg#admin"},
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {}`,
			expectedError: "cannot delete relation `viewer` in object definition `document`, as a relationship exists under it",
		},
		{
			name: "attempt to remove a relation with wildcard",
			startingSchema: `
				definition user {}

				definition document {
					relation viewer: user:* | user
				}`,
			relationships: []string{"document:somedoc#viewer@user:*"},
			endingSchema: `
				definition user {}

				definition document {}`,
			expectedError: "cannot delete relation `viewer` in object definition `document`, as a relationship exists under it",
		},
		{
			name: "attempt to remove a relation with only indirect relationships",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {
					relation viewer: group#member | org#admin
				}`,
			relationships: []string{"document:somedoc#viewer@org:someorg#admin"},
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {}`,
			expectedError: "cannot delete relation `viewer` in object definition `document`, as a relationship exists under it",
		},
		{
			name: "remove a relation with no relationships",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {
					relation viewer: user | group#member | org#admin
				}`,
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition org {
					relation admin: user
				}

				definition document {}`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount: 4,
			},
		},
		{
			name: "change the subject type allowed on a relation",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition document {
					relation viewer: user | group#member
					permission view = viewer
				}
			`,
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition document {
					relation viewer: user
					permission view = viewer
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount: 3,
			},
		},
		{
			name: "attempt to change the subject type allowed on a relation with relationships",
			startingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition document {
					relation viewer: user | group#member
					permission view = viewer
				}
			`,
			relationships: []string{"document:somedoc#viewer@group:somegroup#member"},
			endingSchema: `
				definition user {}

				definition group {
					relation member: user
				}

				definition document {
					relation viewer: user
					permission view = viewer
				}
			`,
			expectedError: "cannot remove allowed type `group#member` from relation `viewer` in object definition `document`, as a relationship exists with it",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			// Write the initial schema.
			relationships := make([]tuple.Relationship, 0, len(tc.relationships))
			for _, rel := range tc.relationships {
				relationships = append(relationships, tuple.MustParse(rel))
			}

			ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, tc.startingSchema, relationships, require)

			// Update the schema and ensure it works.
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.endingSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			validated, err := ValidateSchemaChanges(context.Background(), compiled, false)
			require.NoError(err)

			_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				applied, err := ApplySchemaChanges(context.Background(), rwt, validated)
				if tc.expectedError != "" {
					require.EqualError(err, tc.expectedError)
					return nil
				}

				require.NoError(err)
				require.Equal(tc.expectedAppliedSchemaChanges, *applied)
				return nil
			})
			require.NoError(err)
		})
	}
}
