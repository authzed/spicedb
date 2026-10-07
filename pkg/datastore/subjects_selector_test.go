package datastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestSubjectsSelectorTestMatchesMemdb uses memdb as the oracle for the
// subject relation rules of SubjectsSelector.Test.
func TestSubjectsSelectorTestMatchesMemdb(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	rels := []tuple.Relationship{
		tuple.MustParse("document:doc1#viewer@group:eng"),
		tuple.MustParse("document:doc2#viewer@group:eng#member"),
		tuple.MustParse("document:doc3#viewer@group:eng#viewer"),
		tuple.MustParse("document:doc4#viewer@user:tom"),
	}
	rev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		updates := make([]tuple.RelationshipUpdate, 0, len(rels))
		for _, rel := range rels {
			updates = append(updates, tuple.Create(rel))
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)

	filters := map[string]datastore.SubjectRelationFilter{
		"empty":                {},
		"ellipsis only":        datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
		"non-ellipsis only":    datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
		"ellipsis and member":  datastore.SubjectRelationFilter{}.WithEllipsisRelation().WithNonEllipsisRelation("member"),
		"only non-ellipsis":    datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
		"only non-ellipsis +r": datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations().WithNonEllipsisRelation("viewer"),
	}

	for name, relationFilter := range filters {
		t.Run(name, func(t *testing.T) {
			for _, subjectType := range []string{"", "group"} {
				selector := datastore.SubjectsSelector{
					OptionalSubjectType: subjectType,
					RelationFilter:      relationFilter,
				}

				iter, err := ds.SnapshotReader(rev).QueryRelationships(t.Context(), datastore.RelationshipsFilter{
					OptionalResourceType:      "document",
					OptionalSubjectsSelectors: []datastore.SubjectsSelector{selector},
				})
				require.NoError(t, err)
				found, err := datastore.IteratorToSlice(iter)
				require.NoError(t, err)

				foundSet := map[string]bool{}
				for _, rel := range found {
					foundSet[tuple.MustString(rel)] = true
				}

				for _, rel := range rels {
					require.Equal(t, foundSet[tuple.MustString(rel)], selector.Test(rel.Subject),
						"selector %+v disagrees with memdb for subject %s", selector, tuple.StringONR(rel.Subject))
				}
			}
		})
	}
}
