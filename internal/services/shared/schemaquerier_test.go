package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// sliceQuerier is a RelationshipQuerier over a fixed list of relationships, standing in for the
// querier over the relationships created after a revision.
type sliceQuerier []tuple.Relationship

func (q sliceQuerier) QueryRelationships(_ context.Context, filter datastore.RelationshipsFilter, _ ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	return q.matching(filter.Test), nil
}

func (q sliceQuerier) ReverseQueryRelationships(_ context.Context, subjectsFilter datastore.SubjectsFilter, _ ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	selector := subjectsFilter.AsSelector()
	return q.matching(func(rel tuple.Relationship) bool { return selector.Test(rel.Subject) }), nil
}

func (q sliceQuerier) matching(test func(tuple.Relationship) bool) datastore.RelationshipIterator {
	var matched []tuple.Relationship
	for _, rel := range q {
		if test(rel) {
			matched = append(matched, rel)
		}
	}
	return common.NewSliceRelationshipIterator(matched)
}

// TestApplySchemaChangesWithQuerier checks that the schema change checks run against the given
// querier, and not against the transaction.
func TestApplySchemaChangesWithQuerier(t *testing.T) {
	const startingSchema = `
		definition user {}

		definition document {
			relation viewer: user | user with somecaveat | user:*
			relation editor: user
		}

		caveat somecaveat(somecondition int) {
			somecondition == 42
		}`

	tcs := []struct {
		name          string
		endingSchema  string
		created       []string
		expectedError string
	}{
		{
			name: "removed relation with no created relationships",
			endingSchema: `
				definition user {}
				definition document {
					relation viewer: user | user with somecaveat | user:*
				}
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}`,
		},
		{
			name: "removed relation with a created relationship under it",
			endingSchema: `
				definition user {}
				definition document {
					relation viewer: user | user with somecaveat | user:*
				}
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}`,
			created:       []string{"document:doc2#editor@user:sarah"},
			expectedError: "cannot delete relation `editor` in object definition `document`, as at least one relationship exists under it: document:doc2#editor@user:sarah",
		},
		{
			name: "removed allowed type with a created relationship of another type",
			endingSchema: `
				definition user {}
				definition document {
					relation viewer: user | user:*
					relation editor: user
				}
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}`,
			created: []string{"document:doc2#viewer@user:sarah", "document:doc2#viewer@user:*"},
		},
		{
			name: "removed allowed type with a created relationship of that type",
			endingSchema: `
				definition user {}
				definition document {
					relation viewer: user | user:*
					relation editor: user
				}
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}`,
			created:       []string{"document:doc2#viewer@user:sarah[somecaveat]"},
			expectedError: "cannot remove allowed type `user with somecaveat` from relation `viewer` in object definition `document`, as a relationship exists with it: document:doc2#viewer@user:sarah[somecaveat]",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(t, err)

			// The datastore has a relationship under the removed relation. The checks must not
			// see it: they run only against the created relationships.
			ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, startingSchema, []tuple.Relationship{
				tuple.MustParse("document:doc1#editor@user:tom"),
				tuple.MustParse("document:doc1#viewer@user:tom[somecaveat]"),
			})

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.endingSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			validated, err := ValidateSchemaChanges(t.Context(), compiled, caveattypes.Default.TypeSet, false, tc.endingSchema)
			require.NoError(t, err)

			created := make([]tuple.Relationship, 0, len(tc.created))
			for _, rel := range tc.created {
				created = append(created, tuple.MustParse(rel))
			}

			dl := datalayer.NewDataLayer(ds)
			_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
				_, err := ApplySchemaChangesWithQuerier(ctx, rwt, sliceQuerier(created), caveattypes.Default.TypeSet, validated)
				return err
			})
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
				return
			}
			require.NoError(t, err)
		})
	}
}
