package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
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
			expectedError: "cannot delete relation `viewer` in object definition `document`, as at least one relationship exists under it: document:somedoc#viewer@user:alice",
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
			expectedError: "cannot delete relation `viewer` in object definition `document`, as at least one relationship exists under it: document:somedoc#viewer@group:somegroup#member",
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
			expectedError: "cannot delete relation `viewer` in object definition `document`, as at least one relationship exists under it: document:somedoc#viewer@org:someorg#admin",
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
			expectedError: "cannot delete relation `viewer` in object definition `document`, as at least one relationship exists under it: document:somedoc#viewer@user:*",
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
			expectedError: "cannot delete relation `viewer` in object definition `document`, as at least one relationship exists under it: document:somedoc#viewer@org:someorg#admin",
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
			expectedError: "cannot remove allowed type `group#member` from relation `viewer` in object definition `document`, as a relationship exists with it: document:somedoc#viewer@group:somegroup#member",
		},
		{
			name: "attempt to remove non-caveated type when only caveated relationship exists",
			startingSchema: `
				caveat only_on_tuesday(day_of_week string) {
					day_of_week == 'tuesday'
				}

				definition user {}

				definition document {
					relation writer: user
					relation reader: user | user with only_on_tuesday

					permission edit = writer
					permission view = reader + edit
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom[only_on_tuesday]"},
			endingSchema: `
				caveat only_on_tuesday(day_of_week string) {
					day_of_week == 'tuesday'
				}

				definition user {}

				definition document {
					relation writer: user
					relation reader: user with only_on_tuesday

					permission edit = writer
					permission view = reader + edit
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount: 3,
			},
		},
		{
			name: "attempt to delete a subject type",
			startingSchema: `
				definition user {}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom"},
			endingSchema: `
				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			expectedError: "could not lookup definition `user` for relation `reader`: object definition `user` not found",
		},
		{
			name: "attempt to delete a subject type with a relation",
			startingSchema: `
				definition user {}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom"},
			endingSchema: `
				definition document {
					permission view = nil
				}
			`,
			expectedError: "cannot delete relation `reader` in object definition `document`, as at least one relationship exists under it: document:firstdoc#reader@user:tom",
		},
		{
			name: "delete a subject type with relation but no data",
			startingSchema: `
				definition user {}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			relationships: nil,
			endingSchema: `
				definition document {
					permission view = nil
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount:   2,
				RemovedObjectDefNames: []string{"user"},
			},
		},
		{
			name: "attempt to delete a subject type while adding a replacement",
			startingSchema: `
				definition user {}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom"},
			endingSchema: `
				definition user2 {}

				definition document {
					relation reader: user2
					permission view = reader
				}
			`,
			expectedError: "cannot remove allowed type `user` from relation `reader` in object definition `document`, as a relationship exists with it: document:firstdoc#reader@user:tom",
		},
		{
			name: "delete a subject type while adding a replacement",
			startingSchema: `
				definition user {}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			relationships: nil,
			endingSchema: `
				definition user2 {}

				definition document {
					relation reader: user2
					permission view = reader
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount:   3,
				RemovedObjectDefNames: []string{"user"},
				NewObjectDefNames:     []string{"user2"},
			},
		},
		{
			name: "delete a direct subject type while indirect remains",
			startingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user | user#foo
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom#foo"},
			endingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user#foo
					permission view = reader
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount: 2,
			},
		},
		{
			name: "attempt to delete a direct subject type while indirect remains",
			startingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user | user#foo
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom"},
			endingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user#foo
					permission view = reader
				}
			`,
			expectedError: "cannot remove allowed type `user` from relation `reader` in object definition `document`, as a relationship exists with it: document:firstdoc#reader@user:tom",
		},
		{
			name: "attempt to delete an indirect subject type while direct remains",
			startingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user | user#foo
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom#foo"},
			endingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			expectedError: "cannot remove allowed type `user#foo` from relation `reader` in object definition `document`, as a relationship exists with it: document:firstdoc#reader@user:tom#foo",
		},
		{
			name: "delete an indirect subject type while direct remains",
			startingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user | user#foo
					permission view = reader
				}
			`,
			relationships: []string{"document:firstdoc#reader@user:tom"},
			endingSchema: `
				definition user {
					relation foo: user
				}

				definition document {
					relation reader: user
					permission view = reader
				}
			`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount: 2,
			},
		},
		{
			name: "delete a subject relation when another relationship references the resource type",
			startingSchema: `use expiration

  definition resource {
    relation platform: platform
    relation viewer: user | user:*
  }
  definition platform {
    relation othersubject_thing_doer: othersubject
    permission do_thing = othersubject_thing_doer
  }
  definition othersubject {}
  definition user {}`,
			relationships: []string{
				"resource:oneresource#platform@platform:foo",
				"resource:anotherresource#viewer@user:*",
			},
			endingSchema: `use expiration

definition user {}
definition platform {}
definition resource {
	relation platform: platform
	relation viewer: user | user:*
}`,
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				TotalOperationCount:   4,
				RemovedObjectDefNames: []string{"othersubject"},
			},
		},
		{
			name: "attempt to delete a referenced subject relation",
			startingSchema: `definition document {
				relation viewer: user | user#foo
			}

			definition user {
				relation foo: user
				relation foo2: user
			}`,
			relationships: []string{
				"document:firstdoc#viewer@user:tom#foo",
			},
			endingSchema: `definition document {
				relation viewer: user
			}
			definition user {
				relation foo2: user
			}
			`,
			expectedError: "cannot remove allowed type `user#foo` from relation `viewer` in object definition `document`, as a relationship exists with it: document:firstdoc#viewer@user:tom#foo",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
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

			validated, err := ValidateSchemaChanges(t.Context(), compiled, caveattypes.Default.TypeSet, false, tc.endingSchema)
			if tc.expectedError != "" && err != nil && tc.expectedError == err.Error() {
				return
			}

			require.NoError(err)

			dl := datalayer.NewDataLayer(ds)
			_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
				applied, err := ApplySchemaChanges(t.Context(), rwt, caveattypes.Default.TypeSet, validated)
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

func TestApplySchemaChangesOverExisting(t *testing.T) {
	tcs := []struct {
		name                         string
		staticSchema                 string
		startingSchema               string
		patchSchema                  string
		expectedSchema               string
		expectedAppliedSchemaChanges AppliedSchemaChanges
		expectedError                string
	}{
		{
			name:         "empty static schema",
			staticSchema: "",
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
			patchSchema: `
				definition user {}

				definition organization {
					relation member: user
					permission admin = member
				}

				caveat catchTwentyTwo(value int) {
				value == 22
				}
			`,
			expectedSchema: "caveat catchTwentyTwo(value int) {\n\tvalue == 22\n}\n\ndefinition organization {\n\trelation member: user\n\tpermission admin = member\n}\n\ndefinition user {}",
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				// NOTE: this is 5 because the `user` definition is written even though it's unchanged
				TotalOperationCount:   5,
				NewObjectDefNames:     []string{"organization"},
				RemovedObjectDefNames: []string{"document"},
				NewCaveatDefNames:     []string{"catchTwentyTwo"},
				RemovedCaveatDefNames: []string{"hasFortyTwo"},
			},
		},
		{
			name: "basic static schema",
			staticSchema: `
			definition admin {}
			`,
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
			patchSchema: `
				definition user {}

				definition organization {
					relation member: user
					permission admin = member
				}

				caveat catchTwentyTwo(value int) {
				value == 22
				}
			`,
			expectedSchema: "caveat catchTwentyTwo(value int) {\n\tvalue == 22\n}\n\ndefinition admin {}\n\ndefinition organization {\n\trelation member: user\n\tpermission admin = member\n}\n\ndefinition user {}",
			// NOTE: we're expecting that the `admin` part of the schema stays there.
			expectedAppliedSchemaChanges: AppliedSchemaChanges{
				// NOTE: this is 5 because the `user` definition is written even though it's unchanged
				TotalOperationCount:   5,
				NewObjectDefNames:     []string{"organization"},
				RemovedObjectDefNames: []string{"document"},
				NewCaveatDefNames:     []string{"catchTwentyTwo"},
				RemovedCaveatDefNames: []string{"hasFortyTwo"},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(err)

			// NOTE: the schema that we start with in the DB is the concatenation of the
			// static part of the schema and the written part of the schema because
			// the function under tests takes the "starting" schema (i.e. the part of the schema
			// that's expected to be modified by the written schema) as one of its arguments.
			// It ignores what's already in the database that isn't explicitly named.
			schemaInDB := tc.staticSchema + "\n\n" + tc.startingSchema

			compiledStartingSchema, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.startingSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, schemaInDB, nil, require)

			// Update the schema and ensure it works.
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.patchSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			validated, err := ValidateSchemaChanges(t.Context(), compiled, caveattypes.Default.TypeSet, false, tc.patchSchema)
			if tc.expectedError != "" {
				require.ErrorContains(err, tc.expectedError)
				return
			}

			require.NoError(err)

			dl := datalayer.NewDataLayer(ds)
			_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
				applied, err := ApplySchemaChangesOverExisting(
					t.Context(),
					rwt,
					caveattypes.Default.TypeSet,
					validated,
					compiledStartingSchema.CaveatDefinitions,
					compiledStartingSchema.ObjectDefinitions,
				)
				if tc.expectedError != "" {
					require.EqualError(err, tc.expectedError)
					return nil
				}

				require.NoError(err)
				require.Equal(tc.expectedAppliedSchemaChanges, *applied)

				sr, err := rwt.ReadSchema()
				require.NoError(err)
				schemaText, err := sr.SchemaText()
				require.NoError(err)
				require.Equal(tc.expectedSchema, schemaText)
				return nil
			})
			require.NoError(err)
		})
	}
}
