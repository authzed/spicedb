package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

func TestIndexingHintForQueryShape(t *testing.T) {
	cases := map[queryshape.Shape]struct {
		indexName string
		sort      options.SortOrder
	}{
		queryshape.CheckPermissionSelectDirectSubjects:    {"uq_relation_tuple_living", options.ByResource},
		queryshape.CheckPermissionSelectIndirectSubjects:  {"uq_relation_tuple_living", options.ByResource},
		queryshape.AllSubjectsForResources:                {"uq_relation_tuple_living", options.ByResource},
		queryshape.FindResourceOfType:                     {"uq_relation_tuple_living", options.ByResource},
		queryshape.MatchingResourcesForSubject:            {"ix_relation_tuple_by_subject", options.BySubject},
		queryshape.FindResourceAndSubjectWithRelations:    {"ix_relation_tuple_by_subject_relation", options.BySubject},
		queryshape.FindSubjectOfTypeAndRelation:           {"ix_relation_tuple_by_subject_relation", options.BySubject},
		queryshape.FindResourceRelationForSubjectRelation: {"ix_relation_tuple_by_subject_relation", options.BySubject},
	}

	for shape, expected := range cases {
		t.Run(string(shape), func(t *testing.T) {
			hint := IndexingHintForQueryShape(shape)
			require.NotNil(t, hint)

			suffix, err := hint.FromSQLSuffix()
			require.NoError(t, err)
			require.Equal(t, "FORCE INDEX ("+expected.indexName+")", suffix)

			prefix, err := hint.SQLPrefix()
			require.NoError(t, err)
			require.Empty(t, prefix)

			require.Equal(t, expected.sort, hint.SortOrder())
		})
	}

	require.Nil(t, IndexingHintForQueryShape(queryshape.Varying))
	require.Nil(t, IndexingHintForQueryShape(queryshape.Unspecified))
}
