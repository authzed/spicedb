package queryopt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// applyReachabilityPruning is a test helper that applies the reachability
// pruning transform to a CanonicalOutline, bypassing the registry.
func applyReachabilityPruning(co query.CanonicalOutline, targetSubjectType, targetSubjectRelation string) query.CanonicalOutline {
	co.Root = reachabilityPruning(RequestParams{SubjectType: targetSubjectType, SubjectRelation: targetSubjectRelation})(co.Root)
	return co
}

// dsOutlineForType returns a DatastoreIteratorType outline with the given
// definition, relation, subject type, and subrelation.
func dsOutlineForType(defName, relName, subjectType, subrelation string) query.Outline {
	rel := schema.NewTestBaseRelation(defName, relName, subjectType, subrelation)
	return query.Outline{
		Type: query.DatastoreIteratorType,
		Args: &query.IteratorArgs{Relation: rel},
	}
}

func arrowOutline(left, right query.Outline) query.Outline {
	return query.Outline{
		Type:        query.ArrowIteratorType,
		SubOutlines: []query.Outline{left, right},
	}
}

func exclusionOutline(left, right query.Outline) query.Outline {
	return query.Outline{
		Type:        query.ExclusionIteratorType,
		SubOutlines: []query.Outline{left, right},
	}
}

// canonicalize wraps an outline in a CanonicalOutline with assigned IDs.
func canonicalize(outline query.Outline) query.CanonicalOutline {
	keys := make(map[query.OutlineNodeID]query.CanonicalKey)
	filled := query.FillMissingNodeIDs(outline, keys)
	return query.CanonicalOutline{
		Root:          filled,
		CanonicalKeys: keys,
	}
}

func TestReachabilityPruning(t *testing.T) {
	t.Run("does not prune NullIteratorType", func(t *testing.T) {
		co := canonicalize(query.Outline{Type: query.NullIteratorType})
		result := applyReachabilityPruning(co, "user", "")
		require.Equal(t, query.NullIteratorType, result.Root.Type)
	})

	t.Run("does not prune if subject type is empty", func(t *testing.T) {
		co := canonicalize(dsOutlineForType("document", "viewer", "group", "..."))
		result := applyReachabilityPruning(co, "", "")
		require.Equal(t, query.DatastoreIteratorType, result.Root.Type)
	})

	t.Run("does not prune if subject relation is non-empty", func(t *testing.T) {
		co := canonicalize(dsOutlineForType("document", "viewer", "group", "..."))
		result := applyReachabilityPruning(co, "group", "member")
		require.Equal(t, query.DatastoreIteratorType, result.Root.Type)
	})

	t.Run("prunes leaf with subject type that does not match", func(t *testing.T) {
		co := canonicalize(dsOutlineForType("document", "viewer", "group", "..."))
		result := applyReachabilityPruning(co, "user", "")
		require.Equal(t, query.NullIteratorType, result.Root.Type)
	})

	t.Run("keeps leaf with matching subject type", func(t *testing.T) {
		co := canonicalize(dsOutlineForType("document", "viewer", "user", "..."))
		result := applyReachabilityPruning(co, "user", "")
		require.Equal(t, query.DatastoreIteratorType, result.Root.Type)
	})

	t.Run("union", func(t *testing.T) {
		t.Run("prunes one branch of union", func(t *testing.T) {
			userIt := dsOutlineForType("document", "viewer", "user", "...")
			groupIt := dsOutlineForType("document", "editor", "group", "...")
			co := canonicalize(unionOutline(userIt, groupIt))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.UnionIteratorType, result.Root.Type)
			require.Len(t, result.Root.SubOutlines, 2)
			require.Equal(t, query.DatastoreIteratorType, result.Root.SubOutlines[0].Type, "user branch should remain")
			require.Equal(t, query.NullIteratorType, result.Root.SubOutlines[1].Type, "group branch should be pruned")
		})

		t.Run("does not prune union when both branches match", func(t *testing.T) {
			userIt1 := dsOutlineForType("document", "viewer", "user", "...")
			userIt2 := dsOutlineForType("document", "editor", "user", "...")
			co := canonicalize(unionOutline(userIt1, userIt2))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.UnionIteratorType, result.Root.Type)
			require.Equal(t, query.DatastoreIteratorType, result.Root.SubOutlines[0].Type)
			require.Equal(t, query.DatastoreIteratorType, result.Root.SubOutlines[1].Type)
		})
	})

	t.Run("intersection", func(t *testing.T) {
		t.Run("keeps intersection when all branches match", func(t *testing.T) {
			userIt1 := dsOutlineForType("document", "viewer", "user", "...")
			userIt2 := dsOutlineForType("document", "editor", "user", "...")
			co := canonicalize(intersectionOutline(userIt1, userIt2))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.IntersectionIteratorType, result.Root.Type)
			require.Equal(t, query.DatastoreIteratorType, result.Root.SubOutlines[0].Type)
			require.Equal(t, query.DatastoreIteratorType, result.Root.SubOutlines[1].Type)
		})

		t.Run("prunes entire intersection when one branch doesn't match", func(t *testing.T) {
			userIt := dsOutlineForType("document", "viewer", "user", "...")
			groupIt := dsOutlineForType("document", "editor", "group", "...")
			co := canonicalize(intersectionOutline(userIt, groupIt))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "entire intersection should be pruned because one branch can't produce target type")
		})
	})

	t.Run("arrows", func(t *testing.T) {
		t.Run("prunes entire arrow when right side subject type doesn't match", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "group", "...")
			co := canonicalize(arrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "entire arrow should be pruned")
		})

		t.Run("keeps arrow when right side subject type matches", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "user", "...")
			co := canonicalize(arrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.ArrowIteratorType, result.Root.Type, "arrow should remain")
		})

		t.Run("keeps arrow when right side has multiple subject types and one matches", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			rightUser := dsOutlineForType("folder", "viewer", "user", "...")
			rightGroup := dsOutlineForType("folder", "viewer", "group", "...")
			rightTeam := dsOutlineForType("folder", "viewer", "team", "...")
			right := unionOutline(rightUser, rightGroup, rightTeam)
			co := canonicalize(arrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.ArrowIteratorType, result.Root.Type, "arrow should remain because user is reachable")

			// The right side union should have the group and team branches pruned
			rightResult := result.Root.SubOutlines[1]
			require.Equal(t, query.UnionIteratorType, rightResult.Type)
			require.Len(t, rightResult.SubOutlines, 3)
			require.Equal(t, query.DatastoreIteratorType, rightResult.SubOutlines[0].Type, "user branch should remain")
			require.Equal(t, query.NullIteratorType, rightResult.SubOutlines[1].Type, "group branch should be pruned")
			require.Equal(t, query.NullIteratorType, rightResult.SubOutlines[2].Type, "team branch should be pruned")
		})

		t.Run("prunes arrow when right side has multiple subject types and none match", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			rightGroup := dsOutlineForType("folder", "viewer", "group", "...")
			rightTeam := dsOutlineForType("folder", "viewer", "team", "...")
			rightOrg := dsOutlineForType("folder", "viewer", "org", "...")
			right := unionOutline(rightGroup, rightTeam, rightOrg)
			co := canonicalize(arrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "entire arrow should be pruned")
		})

		t.Run("keeps arrow inside union when reachable", func(t *testing.T) {
			directUser := dsOutlineForType("document", "viewer", "user", "...")
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "user", "...")
			arrow := arrowOutline(left, right)
			co := canonicalize(unionOutline(directUser, arrow))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.UnionIteratorType, result.Root.Type)
			subs := result.Root.SubOutlines
			require.Len(t, subs, 2)
			require.Equal(t, query.DatastoreIteratorType, subs[0].Type, "direct user branch should remain")
			require.Equal(t, query.ArrowIteratorType, subs[1].Type, "arrow should be kept")
		})

		t.Run("prunes arrow inside union when unreachable", func(t *testing.T) {
			directUser := dsOutlineForType("document", "viewer", "user", "...")
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "group", "...")
			arrow := arrowOutline(left, right)
			co := canonicalize(unionOutline(directUser, arrow))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.UnionIteratorType, result.Root.Type)
			subs := result.Root.SubOutlines
			require.Len(t, subs, 2)
			require.Equal(t, query.DatastoreIteratorType, subs[0].Type, "direct user branch should remain")
			require.Equal(t, query.NullIteratorType, subs[1].Type, "arrow branch should be pruned")
		})
	})

	t.Run("intersection arrows", func(t *testing.T) {
		t.Run("prunes entire intersection arrow when right side subject type doesn't match", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "group", "...")
			co := canonicalize(intersectionArrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "entire intersection arrow should be pruned")
		})

		t.Run("keeps intersection arrow when right side subject type matches", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "user", "...")
			co := canonicalize(intersectionArrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.IntersectionArrowIteratorType, result.Root.Type, "intersection arrow should remain")
		})

		t.Run("keeps intersection arrow when right side has multiple subject types and one matches", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			rightUser := dsOutlineForType("folder", "viewer", "user", "...")
			rightGroup := dsOutlineForType("folder", "viewer", "group", "...")
			rightTeam := dsOutlineForType("folder", "viewer", "team", "...")
			right := unionOutline(rightUser, rightGroup, rightTeam)
			co := canonicalize(intersectionArrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.IntersectionArrowIteratorType, result.Root.Type, "intersection arrow should remain")

			rightResult := result.Root.SubOutlines[1]
			require.Equal(t, query.UnionIteratorType, rightResult.Type)
			require.Len(t, rightResult.SubOutlines, 3)
			require.Equal(t, query.DatastoreIteratorType, rightResult.SubOutlines[0].Type, "user branch should remain")
			require.Equal(t, query.NullIteratorType, rightResult.SubOutlines[1].Type, "group branch should be pruned")
			require.Equal(t, query.NullIteratorType, rightResult.SubOutlines[2].Type, "team branch should be pruned")
		})

		t.Run("prunes intersection arrow when right side has multiple subject types and none match", func(t *testing.T) {
			left := dsOutlineForType("document", "parent", "folder", "...")
			rightGroup := dsOutlineForType("folder", "viewer", "group", "...")
			rightTeam := dsOutlineForType("folder", "viewer", "team", "...")
			rightOrg := dsOutlineForType("folder", "viewer", "org", "...")
			right := unionOutline(rightGroup, rightTeam, rightOrg)
			co := canonicalize(intersectionArrowOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "entire intersection arrow should be pruned")
		})

		t.Run("prunes intersection arrow inside union when unreachable", func(t *testing.T) {
			directUser := dsOutlineForType("document", "viewer", "user", "...")
			left := dsOutlineForType("document", "parent", "folder", "...")
			right := dsOutlineForType("folder", "viewer", "group", "...")
			arrow := intersectionArrowOutline(left, right)
			co := canonicalize(unionOutline(directUser, arrow))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.UnionIteratorType, result.Root.Type)
			subs := result.Root.SubOutlines
			require.Len(t, subs, 2)
			require.Equal(t, query.DatastoreIteratorType, subs[0].Type, "direct user branch should remain")
			require.Equal(t, query.NullIteratorType, subs[1].Type, "intersection arrow branch should be pruned")
		})
	})

	t.Run("exclusion", func(t *testing.T) {
		t.Run("prunes entire exclusion when main set on left doesn't match subject type", func(t *testing.T) {
			left := dsOutlineForType("document", "viewer", "group", "...")
			right := dsOutlineForType("document", "blocked", "user", "...")
			co := canonicalize(exclusionOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "entire exclusion should be pruned because left has no matching subject type")
		})

		t.Run("keeps exclusion when main set on left matches one of the subject types", func(t *testing.T) {
			leftUser := dsOutlineForType("document", "viewer", "user", "...")
			leftGroup := dsOutlineForType("document", "viewer", "group", "...")
			left := unionOutline(leftUser, leftGroup)
			right := dsOutlineForType("document", "blocked", "team", "...")
			co := canonicalize(exclusionOutline(left, right))

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.ExclusionIteratorType, result.Root.Type, "exclusion should remain because left has matching subject type")

			// The left union should have the group branch pruned
			leftResult := result.Root.SubOutlines[0]
			require.Equal(t, query.UnionIteratorType, leftResult.Type)
			require.Len(t, leftResult.SubOutlines, 2)
			require.Equal(t, query.DatastoreIteratorType, leftResult.SubOutlines[0].Type, "user branch should remain")
			require.Equal(t, query.NullIteratorType, leftResult.SubOutlines[1].Type, "group branch should be pruned")
		})
	})

	t.Run("self iterator", func(t *testing.T) {
		t.Run("prunes self iterator when definition name doesn't match", func(t *testing.T) {
			co := canonicalize(query.Outline{
				Type: query.SelfIteratorType,
				Args: &query.IteratorArgs{DefinitionName: "group"},
			})

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "self iterator should be pruned")
		})

		t.Run("keeps self iterator when definition name matches", func(t *testing.T) {
			co := canonicalize(query.Outline{
				Type: query.SelfIteratorType,
				Args: &query.IteratorArgs{DefinitionName: "user"},
			})

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.SelfIteratorType, result.Root.Type, "self iterator should remain")
		})
	})

	t.Run("recursive iterator", func(t *testing.T) {
		t.Run("prunes recursive iterator when child doesn't match", func(t *testing.T) {
			child := dsOutlineForType("document", "viewer", "group", "...")
			co := canonicalize(query.Outline{
				Type:        query.RecursiveIteratorType,
				SubOutlines: []query.Outline{child},
			})

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "recursive iterator should be pruned")
		})

		t.Run("keeps recursive iterator when child matches", func(t *testing.T) {
			child := dsOutlineForType("document", "viewer", "user", "...")
			co := canonicalize(query.Outline{
				Type:        query.RecursiveIteratorType,
				SubOutlines: []query.Outline{child},
			})

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.RecursiveIteratorType, result.Root.Type, "recursive iterator should remain")
		})
	})

	t.Run("alias iterator", func(t *testing.T) {
		t.Run("prunes alias iterator when child doesn't match", func(t *testing.T) {
			child := dsOutlineForType("document", "viewer", "group", "...")
			co := canonicalize(query.Outline{
				Type:        query.AliasIteratorType,
				SubOutlines: []query.Outline{child},
			})

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.NullIteratorType, result.Root.Type, "alias iterator should be pruned")
		})

		t.Run("keeps alias iterator when child matches", func(t *testing.T) {
			child := dsOutlineForType("document", "viewer", "user", "...")
			co := canonicalize(query.Outline{
				Type:        query.AliasIteratorType,
				SubOutlines: []query.Outline{child},
			})

			result := applyReachabilityPruning(co, "user", "")
			require.Equal(t, query.AliasIteratorType, result.Root.Type, "alias iterator should remain")
		})
	})
}
