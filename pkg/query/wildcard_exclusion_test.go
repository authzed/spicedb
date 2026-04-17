package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/tuple"
)

func concreteSubjectPath(resourceType, resourceID, relation, subjectType, subjectID string) *Path {
	return &Path{
		Resource: NewObject(resourceType, resourceID),
		Relation: relation,
		Subject: ObjectAndRelation{
			ObjectType: subjectType,
			ObjectID:   subjectID,
			Relation:   tuple.Ellipsis,
		},
	}
}

func collectExclusionPaths(t *testing.T, it Iterator, resource Object) []*Path {
	t.Helper()
	ctx := NewTestContext(t)
	tripTopLevel(t, ctx, resource)
	seq, err := ctx.IterSubjects(it, resource, NoObjectFilter())
	require.NoError(t, err)
	paths, err := CollectAll(seq)
	require.NoError(t, err)
	return paths
}

// TestCollectExcludedSubjects exercises the new helper added alongside the wildcard
// refactor (exclusion.go:collectExcludedSubjects). It merges a map of exclusions
// with any previously-tracked exclusions and returns nil when both are empty.
func TestCollectExcludedSubjects(t *testing.T) {
	alice := concreteSubjectPath("document", "doc1", "banned", "user", "alice")
	bob := concreteSubjectPath("document", "doc1", "banned", "user", "bob")
	charlie := concreteSubjectPath("document", "doc1", "banned", "user", "charlie")

	t.Run("EmptyMapAndEmptyExisting_ReturnsNil", func(t *testing.T) {
		result := collectExcludedSubjects(nil, nil)
		require.Nil(t, result)
	})

	t.Run("EmptyMap_WithExisting_ReturnsNil", func(t *testing.T) {
		// When both are empty the helper short-circuits to nil; existing empty
		// slice triggers the len()==0 branch as well.
		result := collectExcludedSubjects(map[string]*Path{}, []*Path{})
		require.Nil(t, result)
	})

	t.Run("NonEmptyMap_NoExisting_ReturnsMapEntries", func(t *testing.T) {
		excludedMap := map[string]*Path{
			ObjectAndRelationKey(alice.Subject): alice,
			ObjectAndRelationKey(bob.Subject):   bob,
		}
		result := collectExcludedSubjects(excludedMap, nil)
		require.Len(t, result, 2)
		ids := slicez.Map(result, func(p *Path) string { return p.Subject.ObjectID })
		require.ElementsMatch(t, []string{"alice", "bob"}, ids)
	})

	t.Run("NonEmptyMap_WithExisting_PreservesExistingFirst", func(t *testing.T) {
		excludedMap := map[string]*Path{
			ObjectAndRelationKey(bob.Subject): bob,
		}
		existing := []*Path{charlie}
		result := collectExcludedSubjects(excludedMap, existing)
		require.Len(t, result, 2)
		// Existing items are appended first; map entries come after.
		require.Equal(t, "charlie", result[0].Subject.ObjectID)
		require.Equal(t, "bob", result[1].Subject.ObjectID)
	})
}

// TestExclusionIterator_WildcardBehavior exercises the wildcard branches of
// ExclusionIterator.IterSubjectsImpl (exclusion.go:122) via FixedIterator inputs.
// Results are collected without top-level wildcard filtering so wildcard paths
// and their ExcludedSubjects tracking can be asserted directly.
func TestExclusionIterator_WildcardBehavior(t *testing.T) {
	resource := NewObject("document", "doc1")
	rel := "view"

	concrete := func(id string) Path {
		return *concreteSubjectPath("document", "doc1", rel, "user", id)
	}
	wc := func() Path { return *wildcardPath("document", "doc1", rel, "user") }

	t.Run("ConcreteMain_WildcardExcluded_ExcludesEverything", func(t *testing.T) {
		// main={alice, bob}, excluded={*} (uncaveated): both concrete subjects
		// are excluded because combineExclusionCaveats treats "both uncaveated"
		// as a complete exclusion.
		main := NewFixedIterator(concrete("alice"), concrete("bob"))
		excluded := NewFixedIterator(wc())
		it := NewExclusionIterator(main, excluded)

		paths := collectExclusionPaths(t, it, resource)
		require.Empty(t, paths)
	})

	t.Run("WildcardMain_ConcreteExcluded_TracksExcludedSubjects", func(t *testing.T) {
		// main={*}, excluded={alice}: wildcard passes through main, and its
		// ExcludedSubjects field records alice so downstream operators (e.g.
		// nested exclusion) can "escape" alice.
		main := NewFixedIterator(wc())
		excluded := NewFixedIterator(concrete("alice"))
		it := NewExclusionIterator(main, excluded)

		paths := collectExclusionPaths(t, it, resource)
		require.Len(t, paths, 1)
		require.Equal(t, tuple.PublicWildcard, paths[0].Subject.ObjectID)
		require.Len(t, paths[0].ExcludedSubjects, 1)
		require.Equal(t, "alice", paths[0].ExcludedSubjects[0].Subject.ObjectID)
	})

	t.Run("WildcardMain_WildcardExcluded_NoEscapes_ExcludesAll", func(t *testing.T) {
		// main={*}, excluded={*}: both uncaveated wildcards cancel.
		main := NewFixedIterator(wc())
		excluded := NewFixedIterator(wc())
		it := NewExclusionIterator(main, excluded)

		paths := collectExclusionPaths(t, it, resource)
		require.Empty(t, paths)
	})

	t.Run("WildcardMain_WildcardExcludedWithEscapes_YieldsEscaped", func(t *testing.T) {
		// main={*}, excluded={* with ExcludedSubjects=[sarah]}.
		// The wildcards cancel, but sarah escapes back into the result.
		mainPath := wc()
		excludedWildcard := wc()
		sarah := concreteSubjectPath("document", "doc1", rel, "user", "sarah")
		excludedWildcard.ExcludedSubjects = []*Path{sarah}

		main := NewFixedIterator(mainPath)
		excluded := NewFixedIterator(excludedWildcard)
		it := NewExclusionIterator(main, excluded)

		paths := collectExclusionPaths(t, it, resource)
		require.Len(t, paths, 1)
		require.Equal(t, "sarah", paths[0].Subject.ObjectID)
	})

	t.Run("ConcreteMain_SpecificConcreteExcluded_AndWildcardExcluded", func(t *testing.T) {
		// main={alice, bob}, excluded={alice(caveated), *(caveated)}:
		// alice hits specific exclusion path (her caveat applied first);
		// bob falls through to wildcard exclusion branch.
		aliceCaveated := *concreteSubjectPath("document", "doc1", rel, "user", "alice")
		aliceCaveated.Caveat = caveats.CaveatExprForTesting("alice_cav")
		wildcardCaveated := wc()
		wildcardCaveated.Caveat = caveats.CaveatExprForTesting("wildcard_cav")

		main := NewFixedIterator(concrete("alice"), concrete("bob"))
		excluded := NewFixedIterator(aliceCaveated, wildcardCaveated)
		it := NewExclusionIterator(main, excluded)

		paths := collectExclusionPaths(t, it, resource)
		ids := slicez.Map(paths, func(p *Path) string { return p.Subject.ObjectID })
		// Both alice and bob survive with caveat annotations.
		require.ElementsMatch(t, []string{"alice", "bob"}, ids)
		for _, p := range paths {
			require.NotNil(t, p.Caveat, "expected caveat annotation on %s", p.Subject.ObjectID)
		}
	})
}
