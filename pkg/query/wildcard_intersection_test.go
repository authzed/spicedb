package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/tuple"
)

// wildcardPath constructs a path with a wildcard subject of the given type
// scoped to the given resource.
func wildcardPath(resourceType, resourceID, relation, subjectType string) *Path {
	return &Path{
		Resource: NewObject(resourceType, resourceID),
		Relation: relation,
		Subject: ObjectAndRelation{
			ObjectType: subjectType,
			ObjectID:   tuple.PublicWildcard,
			Relation:   tuple.Ellipsis,
		},
	}
}

// tripTopLevel makes a throwaway IterSubjects call so subsequent calls on the same
// Context are NOT treated as top-level (and so wildcard subjects are not stripped
// by FilterWildcardSubjects). Needed to observe the internal wildcard semantics
// of Intersection/Exclusion iterators.
func tripTopLevel(t *testing.T, ctx *Context, resource Object) {
	t.Helper()
	empty := NewFixedIterator()
	_, err := ctx.IterSubjects(empty, resource, NoObjectFilter())
	require.NoError(t, err)
}

func collectIntersectionPaths(t *testing.T, it Iterator, resource Object) []*Path {
	t.Helper()
	ctx := NewTestContext(t)
	tripTopLevel(t, ctx, resource)
	seq, err := ctx.IterSubjects(it, resource, NoObjectFilter())
	require.NoError(t, err)
	paths, err := CollectAll(seq)
	require.NoError(t, err)
	return paths
}

// TestIntersectSubjectSets exercises the wildcard-aware intersection helper
// (intersection.go:intersectSubjectSets) directly. The helper is the core of
// the wildcard refactor and has four combinatorial branches: concrete∩concrete,
// concrete∩wildcard, wildcard∩concrete, wildcard∩wildcard.
func TestIntersectSubjectSets(t *testing.T) {
	resource := NewObject("document", "doc1")
	relation := "view"

	concretePath := func(id string, caveat string) *Path {
		p := &Path{
			Resource: resource,
			Relation: relation,
			Subject: ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   id,
				Relation:   tuple.Ellipsis,
			},
		}
		if caveat != "" {
			p.Caveat = caveats.CaveatExprForTesting(caveat)
		}
		return p
	}
	wildcard := func(caveat string) *Path {
		p := wildcardPath("document", "doc1", relation, "user")
		if caveat != "" {
			p.Caveat = caveats.CaveatExprForTesting(caveat)
		}
		return p
	}
	keyOf := func(p *Path) string { return ObjectAndRelationKey(p.Subject) }

	t.Run("ConcreteIntersectConcrete", func(t *testing.T) {
		alice := concretePath("alice", "")
		bob := concretePath("bob", "")
		charlie := concretePath("charlie", "")

		prev := map[string]*Path{keyOf(alice): alice, keyOf(bob): bob}
		curr := map[string]*Path{keyOf(bob): bob, keyOf(charlie): charlie}

		result, resultWildcard, err := intersectSubjectSets(prev, nil, curr, nil)
		require.NoError(t, err)
		require.Nil(t, resultWildcard)
		require.Len(t, result, 1)
		_, hasBob := result[keyOf(bob)]
		require.True(t, hasBob, "only bob is in both sides")
	})

	t.Run("ConcreteIntersectWildcard", func(t *testing.T) {
		alice := concretePath("alice", "")
		bob := concretePath("bob", "")
		prev := map[string]*Path{keyOf(alice): alice, keyOf(bob): bob}

		// curr has only a wildcard: every concrete in prev should survive.
		result, resultWildcard, err := intersectSubjectSets(prev, nil, nil, wildcard(""))
		require.NoError(t, err)
		require.Nil(t, resultWildcard)
		require.Len(t, result, 2)
		require.Contains(t, result, keyOf(alice))
		require.Contains(t, result, keyOf(bob))
	})

	t.Run("WildcardIntersectConcrete", func(t *testing.T) {
		alice := concretePath("alice", "")
		bob := concretePath("bob", "")
		curr := map[string]*Path{keyOf(alice): alice, keyOf(bob): bob}

		// prev has only a wildcard: every concrete in curr should survive.
		result, resultWildcard, err := intersectSubjectSets(nil, wildcard(""), curr, nil)
		require.NoError(t, err)
		require.Nil(t, resultWildcard)
		require.Len(t, result, 2)
		require.Contains(t, result, keyOf(alice))
		require.Contains(t, result, keyOf(bob))
	})

	t.Run("WildcardIntersectWildcard", func(t *testing.T) {
		result, resultWildcard, err := intersectSubjectSets(nil, wildcard(""), nil, wildcard(""))
		require.NoError(t, err)
		require.Empty(t, result)
		require.NotNil(t, resultWildcard)
		require.Equal(t, tuple.PublicWildcard, resultWildcard.Subject.ObjectID)
	})

	t.Run("CaveatANDMerging_ConcreteMatchedByCurrWildcard", func(t *testing.T) {
		alice := concretePath("alice", "alice_caveat")
		prev := map[string]*Path{keyOf(alice): alice}

		result, _, err := intersectSubjectSets(prev, nil, nil, wildcard("wildcard_caveat"))
		require.NoError(t, err)
		require.Len(t, result, 1)
		merged := result[keyOf(alice)]
		require.NotNil(t, merged.Caveat)
		// Must be an AND of both caveats.
		require.Equal(t, "AND", merged.Caveat.GetOperation().GetOp().String())
		require.Len(t, merged.Caveat.GetOperation().GetChildren(), 2)
	})

	t.Run("CaveatANDMerging_ConcreteMatchedByPrevWildcard", func(t *testing.T) {
		bob := concretePath("bob", "bob_caveat")
		curr := map[string]*Path{keyOf(bob): bob}

		result, _, err := intersectSubjectSets(nil, wildcard("prev_wildcard_caveat"), curr, nil)
		require.NoError(t, err)
		require.Len(t, result, 1)
		merged := result[keyOf(bob)]
		require.NotNil(t, merged.Caveat)
		require.Equal(t, "AND", merged.Caveat.GetOperation().GetOp().String())
	})

	t.Run("CaveatANDMerging_WildcardIntersectWildcard", func(t *testing.T) {
		_, resultWildcard, err := intersectSubjectSets(nil, wildcard("a"), nil, wildcard("b"))
		require.NoError(t, err)
		require.NotNil(t, resultWildcard)
		require.NotNil(t, resultWildcard.Caveat)
		require.Equal(t, "AND", resultWildcard.Caveat.GetOperation().GetOp().String())
	})

	t.Run("ConcreteIntersectConcrete_DoesNotUseWildcardOverlap", func(t *testing.T) {
		// When a subject exists in both the concrete∩concrete branch AND would also
		// be matched by a wildcard on either side, the concrete∩concrete rule wins
		// (caveats AND'd from both concrete rows, not overlaid with wildcard caveat).
		alice := concretePath("alice", "a")
		aliceAgain := concretePath("alice", "b")
		prev := map[string]*Path{keyOf(alice): alice}
		curr := map[string]*Path{keyOf(aliceAgain): aliceAgain}

		result, _, err := intersectSubjectSets(prev, wildcard("prev_wc"), curr, wildcard("curr_wc"))
		require.NoError(t, err)
		require.Len(t, result, 1)
		merged := result[keyOf(alice)]
		require.NotNil(t, merged.Caveat)
		// Caveat should be AND(a, b) — not involving either wildcard caveat.
		require.Equal(t, "AND", merged.Caveat.GetOperation().GetOp().String())
	})
}

// TestIntersectionIterator_WildcardBehavior exercises IterSubjectsImpl end-to-end
// through the Context via FixedIterator inputs containing wildcards.
func TestIntersectionIterator_WildcardBehavior(t *testing.T) {
	resource := NewObject("document", "doc1")

	makeConcrete := func(id string) Path {
		return Path{
			Resource: resource,
			Relation: "viewer",
			Subject: ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   id,
				Relation:   tuple.Ellipsis,
			},
		}
	}
	makeWildcard := func() Path { return *wildcardPath("document", "doc1", "viewer", "user") }

	t.Run("WildcardOnOneSide_ConcreteOnOther_YieldsConcrete", func(t *testing.T) {
		left := NewFixedIterator(makeWildcard())
		right := NewFixedIterator(makeConcrete("alice"), makeConcrete("bob"))
		it := NewIntersectionIterator(left, right)

		paths := collectIntersectionPaths(t, it, resource)
		ids := slicez.Map(paths, func(p *Path) string { return p.Subject.ObjectID })
		require.ElementsMatch(t, []string{"alice", "bob"}, ids)
	})

	t.Run("WildcardOnBothSides_YieldsWildcard", func(t *testing.T) {
		left := NewFixedIterator(makeWildcard())
		right := NewFixedIterator(makeWildcard())
		it := NewIntersectionIterator(left, right)

		paths := collectIntersectionPaths(t, it, resource)
		require.Len(t, paths, 1)
		require.Equal(t, tuple.PublicWildcard, paths[0].Subject.ObjectID)
	})

	t.Run("ConcreteWildcardMix_YieldsUnion", func(t *testing.T) {
		// Left: {*, alice}; Right: {*, bob}
		// Concrete∩concrete: none (alice not in right, bob not in left)
		// Concrete∩right-wildcard: alice survives
		// Left-wildcard∩concrete: bob survives
		// Wildcard∩wildcard: * also survives
		left := NewFixedIterator(makeWildcard(), makeConcrete("alice"))
		right := NewFixedIterator(makeWildcard(), makeConcrete("bob"))
		it := NewIntersectionIterator(left, right)

		paths := collectIntersectionPaths(t, it, resource)
		ids := slicez.Map(paths, func(p *Path) string { return p.Subject.ObjectID })
		require.ElementsMatch(t, []string{tuple.PublicWildcard, "alice", "bob"}, ids)
	})

	t.Run("EmptyIteratorShortCircuits", func(t *testing.T) {
		left := NewFixedIterator(makeWildcard())
		right := NewFixedIterator() // empty
		it := NewIntersectionIterator(left, right)

		paths := collectIntersectionPaths(t, it, resource)
		require.Empty(t, paths)
	})
}

// TestIntersectionIterator_IterResources_Wildcard exercises IterResourcesImpl
// (intersection.go:213) including a case where one branch yields multiple paths
// for the same resource with different subjects (tests the "keep the first" branch).
func TestIntersectionIterator_IterResources_Wildcard(t *testing.T) {
	ctx := NewTestContext(t)
	subject := NewObject("user", "alice").WithEllipses()

	mkPath := func(resID, subjID string) Path {
		return Path{
			Resource: NewObject("document", resID),
			Relation: "viewer",
			Subject: ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   subjID,
				Relation:   tuple.Ellipsis,
			},
		}
	}

	// Both branches have alice for doc1, only one has doc2.
	left := NewFixedIterator(mkPath("doc1", "alice"), mkPath("doc2", "alice"))
	right := NewFixedIterator(mkPath("doc1", "alice"))
	it := NewIntersectionIterator(left, right)

	seq, err := ctx.IterResources(it, subject, NoObjectFilter())
	require.NoError(t, err)
	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, "doc1", paths[0].Resource.ObjectID)
}
