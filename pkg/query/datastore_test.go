package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// createTestBaseRelation creates a BaseRelation for testing with the proper parent structure
func createTestBaseRelation(defName, relationName, subjectType, subrelation string) *schema.BaseRelation {
	// Create the schema hierarchy: Schema -> Definition -> Relation -> BaseRelation
	testSchema := &schema.Schema{
		Definitions: make(map[string]*schema.Definition),
	}

	testDefinition := &schema.Definition{
		Parent:    testSchema,
		Name:      defName,
		Relations: make(map[string]*schema.Relation),
	}
	testSchema.Definitions[defName] = testDefinition

	testRelation := &schema.Relation{
		Parent: testDefinition,
		Name:   relationName,
	}
	testDefinition.Relations[relationName] = testRelation

	return &schema.BaseRelation{
		Parent:      testRelation,
		Type:        subjectType,
		Subrelation: subrelation,
		Caveat:      "",
		Expiration:  false,
	}
}

// createTestBaseRelationWithFeatures creates a BaseRelation with caveat and expiration features
func createTestBaseRelationWithFeatures(defName, relationName, subjectType, subrelation, caveat string, expiration bool) *schema.BaseRelation {
	baseRel := createTestBaseRelation(defName, relationName, subjectType, subrelation)
	baseRel.Caveat = caveat
	baseRel.Expiration = expiration
	return baseRel
}

func TestRelationIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("SubjectTypeMismatchReturnsEmpty", func(t *testing.T) {
		t.Parallel()

		// Create a base relation that expects "user" type subjects
		baseRel := createTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
		relationIter := NewRelationIterator(baseRel)

		// Test with mismatched subject type - this should return empty due to the bug fix
		// without hitting the datastore (early return)
		relSeq, err := relationIter.CheckImpl(ctx, NewObjects("document", "doc1"), NewObject("group", "engineers").WithEllipses())
		require.NoError(err, "CheckImpl should not error on type mismatch")

		rels, err := CollectAll(relSeq)
		require.NoError(err, "Collecting empty sequence should not error")
		require.Empty(rels, "Subject type mismatch should return empty results")

		// Test with another mismatched subject type
		relSeq, err = relationIter.CheckImpl(ctx, NewObjects("document", "doc1"), NewObject("organization", "company").WithEllipses())
		require.NoError(err, "CheckImpl should not error on type mismatch")

		rels, err = CollectAll(relSeq)
		require.NoError(err, "Collecting empty sequence should not error")
		require.Empty(rels, "Subject type mismatch should return empty results")

		// Note: We don't test the matching case here because it requires a real datastore
		// The important bug fix is the early return for type mismatches, which we've tested above
	})

	t.Run("Clone", func(t *testing.T) {
		t.Parallel()

		baseRel := createTestBaseRelation("document", "editor", "user", tuple.Ellipsis)
		original := NewRelationIterator(baseRel)
		cloned := original.Clone()

		require.NotSame(original, cloned, "cloned iterator should be a different object")

		// Verify the cloned iterator has the same base relation
		// Both should be RelationIterator instances with the same base
		require.IsType(original, cloned, "cloned should be same type as original")

		// Since both are RelationIterator, we can compare their Explain output which includes the base relation info
		require.Equal(original.Explain().Info, cloned.Explain().Info, "cloned iterator should have same base relation")
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicRelation", func(t *testing.T) {
			t.Parallel()

			baseRel := createTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
			relationIter := NewRelationIterator(baseRel)
			explain := relationIter.Explain()

			expected := "Relation(document:viewer -> user:\"...\", caveat: \"\", expiration: false)"
			require.Equal(expected, explain.Info)
			require.Empty(explain.SubExplain, "RelationIterator should have no sub-explains")
		})

		t.Run("RelationWithCaveat", func(t *testing.T) {
			t.Parallel()

			baseRel := createTestBaseRelationWithFeatures("document", "conditional_viewer", "user", tuple.Ellipsis, "test_caveat", false)
			relationIter := NewRelationIterator(baseRel)
			explain := relationIter.Explain()

			expected := "Relation(document:conditional_viewer -> user:\"...\", caveat: \"test_caveat\", expiration: false)"
			require.Equal(expected, explain.Info)
		})

		t.Run("RelationWithExpiration", func(t *testing.T) {
			t.Parallel()

			baseRel := createTestBaseRelationWithFeatures("document", "temp_viewer", "user", tuple.Ellipsis, "", true)
			relationIter := NewRelationIterator(baseRel)
			explain := relationIter.Explain()

			expected := "Relation(document:temp_viewer -> user:\"...\", caveat: \"\", expiration: true)"
			require.Equal(expected, explain.Info)
		})

		t.Run("RelationWithSpecificSubrelation", func(t *testing.T) {
			t.Parallel()

			baseRel := createTestBaseRelation("document", "parent", "folder", "member")
			relationIter := NewRelationIterator(baseRel)
			explain := relationIter.Explain()

			expected := "Relation(document:parent -> folder:\"member\", caveat: \"\", expiration: false)"
			require.Equal(expected, explain.Info)
		})
	})
}

func TestRelationIteratorSubjectTypeMismatchScenarios(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	// Test only mismatched types since matching types require a real datastore
	testCases := []struct {
		name                string
		expectedSubjectType string
		actualSubjectType   string
	}{
		{
			name:                "MismatchUserToGroup",
			expectedSubjectType: "user",
			actualSubjectType:   "group",
		},
		{
			name:                "MismatchGroupToUser",
			expectedSubjectType: "group",
			actualSubjectType:   "user",
		},
		{
			name:                "MismatchUserToDocument",
			expectedSubjectType: "user",
			actualSubjectType:   "document",
		},
		{
			name:                "MismatchDocumentToUser",
			expectedSubjectType: "document",
			actualSubjectType:   "user",
		},
		{
			name:                "MismatchGroupToDocument",
			expectedSubjectType: "group",
			actualSubjectType:   "document",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			baseRel := createTestBaseRelation("document", "viewer", tc.expectedSubjectType, tuple.Ellipsis)
			relationIter := NewRelationIterator(baseRel)

			subject := NewObject(tc.actualSubjectType, "test_id").WithEllipses()

			// All test cases are mismatched types that should return empty without hitting datastore
			relSeq, err := relationIter.CheckImpl(ctx, NewObjects("document", "doc1"), subject)
			require.NoError(err, "CheckImpl should not error on type mismatch for case %s", tc.name)

			rels, err := CollectAll(relSeq)
			require.NoError(err, "CollectAll should not error for case %s", tc.name)
			require.Empty(rels, "Should return empty for case %s", tc.name)
		})
	}
}
