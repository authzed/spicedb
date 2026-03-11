package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schema/v2"
)

func dsIter(defName, relName, subjectType, subrelation string) *DatastoreIterator {
	return NewDatastoreIterator(schema.NewTestBaseRelation(defName, relName, subjectType, subrelation))
}

func TestPruneUnreachableSubjectTypes(t *testing.T) {
	t.Parallel()

	t.Run("does not prune empty FixedIterator", func(t *testing.T) {
		t.Parallel()

		it := NewFixedIterator()

		result, changed, err := ApplyReachabilityPruning(it, "user")
		require.NoError(t, err)
		require.False(t, changed)
		require.IsType(t, &FixedIterator{}, result)
	})

	t.Run("preserves canonical key on pruned iterator", func(t *testing.T) {
		t.Parallel()

		it := dsIter("document", "viewer", "group", "...")
		it.canonicalKey = CanonicalKey("test-key")

		result, changed, err := ApplyReachabilityPruning(it, "user")
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, CanonicalKey("test-key"), result.CanonicalKey())
	})

	t.Run("prunes leaf with subject type that does not match", func(t *testing.T) {
		t.Parallel()

		it := dsIter("document", "viewer", "group", "...")

		result, changed, err := ApplyReachabilityPruning(it, "user")
		require.NoError(t, err)
		require.True(t, changed)
		require.IsType(t, &FixedIterator{}, result)
		require.Empty(t, result.(*FixedIterator).paths)
	})

	t.Run("keeps leaf with matching subject type", func(t *testing.T) {
		t.Parallel()

		it := dsIter("document", "viewer", "user", "...")

		result, changed, err := ApplyReachabilityPruning(it, "user")
		require.NoError(t, err)
		require.False(t, changed)
		require.IsType(t, &DatastoreIterator{}, result)
	})

	t.Run("union", func(t *testing.T) {
		t.Run("prunes one branch of union", func(t *testing.T) {
			t.Parallel()

			userIt := dsIter("document", "viewer", "user", "...")
			groupIt := dsIter("document", "editor", "group", "...")
			union := NewUnionIterator(userIt, groupIt)

			result, changed, err := ApplyReachabilityPruning(union, "user")
			require.NoError(t, err)
			require.True(t, changed)

			require.IsType(t, &UnionIterator{}, result)
			subs := result.Subiterators()
			require.Len(t, subs, 2)
			require.IsType(t, &DatastoreIterator{}, subs[0], "user branch should remain")
			require.IsType(t, &FixedIterator{}, subs[1], "group branch should be pruned")
		})

		t.Run("does not prune union when both branches match the subject type", func(t *testing.T) {
			t.Parallel()

			userIt1 := dsIter("document", "viewer", "user", "...")
			userIt2 := dsIter("document", "editor", "user", "...")
			union := NewUnionIterator(userIt1, userIt2)

			result, changed, err := ApplyReachabilityPruning(union, "user")
			require.NoError(t, err)
			require.False(t, changed)
			require.IsType(t, &UnionIterator{}, result)
		})
	})

	t.Run("arrows", func(t *testing.T) {
		t.Run("prunes entire arrow when right side subject type doesn't match", func(t *testing.T) {
			t.Parallel()

			// Arrow[document#parent->folder, folder#viewer->group]
			// Right side produces group subjects, not user, so the whole arrow should be replaced
			left := dsIter("document", "parent", "folder", "...")
			right := dsIter("folder", "viewer", "group", "...")
			arrow := NewArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.True(t, changed)
			require.IsType(t, &FixedIterator{}, result, "entire arrow should be pruned")
		})

		t.Run("keeps arrow when right side subject type matches", func(t *testing.T) {
			t.Parallel()

			// Arrow[document#parent->folder, folder#viewer->user]
			// Right side produces user subjects - matches target.
			left := dsIter("document", "parent", "folder", "...")
			right := dsIter("folder", "viewer", "user", "...")
			arrow := NewArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.False(t, changed)
			require.IsType(t, &ArrowIterator{}, result, "arrow should remain")
		})

		t.Run("keeps arrow when right side has multiple subject types and one matches", func(t *testing.T) {
			t.Parallel()

			// Arrow[document#parent->folder, Union[folder#viewer->user, folder#viewer->group, folder#viewer->team]]
			// Right side produces user, group, and team subjects. User matches target.
			// The non-matching branches (group, team) inside the union get pruned,
			// so changed=true, but the arrow itself is kept.
			left := dsIter("document", "parent", "folder", "...")
			rightUser := dsIter("folder", "viewer", "user", "...")
			rightGroup := dsIter("folder", "viewer", "group", "...")
			rightTeam := dsIter("folder", "viewer", "team", "...")
			right := NewUnionIterator(rightUser, rightGroup, rightTeam)
			arrow := NewArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.True(t, changed, "non-matching branches inside the right-side union are pruned")
			require.IsType(t, &ArrowIterator{}, result, "arrow should remain because user is reachable")

			// The right side union should have the group and team branches pruned
			rightResult := result.Subiterators()[1]
			require.IsType(t, &UnionIterator{}, rightResult)
			rightSubs := rightResult.Subiterators()
			require.Len(t, rightSubs, 3)
			require.IsType(t, &DatastoreIterator{}, rightSubs[0], "user branch should remain")
			require.IsType(t, &FixedIterator{}, rightSubs[1], "group branch should be pruned")
			require.IsType(t, &FixedIterator{}, rightSubs[2], "team branch should be pruned")
		})

		t.Run("prunes arrow when right side has multiple subject types and none match", func(t *testing.T) {
			t.Parallel()

			// Arrow[document#parent->folder, Union[folder#viewer->group, folder#viewer->team, folder#viewer->org]]
			// Right side produces group, team, and org subjects. None match user.
			left := dsIter("document", "parent", "folder", "...")
			rightGroup := dsIter("folder", "viewer", "group", "...")
			rightTeam := dsIter("folder", "viewer", "team", "...")
			rightOrg := dsIter("folder", "viewer", "org", "...")
			right := NewUnionIterator(rightGroup, rightTeam, rightOrg)
			arrow := NewArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.True(t, changed)
			require.IsType(t, &FixedIterator{}, result, "entire arrow should be pruned")
		})

		t.Run("prunes arrow inside union when unreachable", func(t *testing.T) {
			t.Parallel()

			// Union[
			//   document#viewer->user,
			//   Arrow[document#parent->folder, folder#viewer->group]
			// ]
			// The arrow's right side produces group, not user, so arrow should get pruned.
			directUser := dsIter("document", "viewer", "user", "...")
			left := dsIter("document", "parent", "folder", "...")
			right := dsIter("folder", "viewer", "group", "...")
			arrow := NewArrowIterator(left, right)
			union := NewUnionIterator(directUser, arrow)

			result, changed, err := ApplyReachabilityPruning(union, "user")
			require.NoError(t, err)
			require.True(t, changed)

			require.IsType(t, &UnionIterator{}, result)
			subs := result.Subiterators()
			require.Len(t, subs, 2)
			require.IsType(t, &DatastoreIterator{}, subs[0], "direct user branch should remain")
			require.IsType(t, &FixedIterator{}, subs[1], "arrow branch should be pruned")
		})
	})

	t.Run("intersection arrows", func(t *testing.T) {
		t.Run("prunes entire intersection arrow when right side subject type doesn't match", func(t *testing.T) {
			t.Parallel()

			// IntersectionArrow[document#parent->folder, folder#viewer->group]
			// Right side produces group subjects, not user, so the whole IntersectionArrow should be pruned
			left := dsIter("document", "parent", "folder", "...")
			right := dsIter("folder", "viewer", "group", "...")
			arrow := NewIntersectionArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.True(t, changed)
			require.IsType(t, &FixedIterator{}, result, "entire intersection arrow should be pruned")
		})

		t.Run("keeps intersection arrow when right side subject type matches", func(t *testing.T) {
			t.Parallel()

			// IntersectionArrow[document#parent->folder, folder#viewer->user]
			// Right side produces user subjects - matches target.
			left := dsIter("document", "parent", "folder", "...")
			right := dsIter("folder", "viewer", "user", "...")
			arrow := NewIntersectionArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.False(t, changed)
			require.IsType(t, &IntersectionArrowIterator{}, result, "intersection arrow should remain")
		})

		t.Run("keeps intersection arrow when right side has multiple subject types and one matches", func(t *testing.T) {
			t.Parallel()

			// IntersectionArrow[document#parent->folder, Union[folder#viewer->user, folder#viewer->group, folder#viewer->team]]
			// Right side produces user, group, and team subjects. User matches target.
			// The non-matching branches (group, team) inside the union get pruned,
			// so changed=true, but the intersection arrow itself is kept.
			left := dsIter("document", "parent", "folder", "...")
			rightUser := dsIter("folder", "viewer", "user", "...")
			rightGroup := dsIter("folder", "viewer", "group", "...")
			rightTeam := dsIter("folder", "viewer", "team", "...")
			right := NewUnionIterator(rightUser, rightGroup, rightTeam)
			arrow := NewIntersectionArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.True(t, changed, "non-matching branches inside the right-side union are pruned")
			require.IsType(t, &IntersectionArrowIterator{}, result, "intersection arrow should remain because user is reachable")

			// The right side union should have the group and team branches pruned
			rightResult := result.Subiterators()[1]
			require.IsType(t, &UnionIterator{}, rightResult)
			rightSubs := rightResult.Subiterators()
			require.Len(t, rightSubs, 3)
			require.IsType(t, &DatastoreIterator{}, rightSubs[0], "user branch should remain")
			require.IsType(t, &FixedIterator{}, rightSubs[1], "group branch should be pruned")
			require.IsType(t, &FixedIterator{}, rightSubs[2], "team branch should be pruned")
		})

		t.Run("prunes intersection arrow when right side has multiple subject types and none match", func(t *testing.T) {
			t.Parallel()

			// IntersectionArrow[document#parent->folder, Union[folder#viewer->group, folder#viewer->team, folder#viewer->org]]
			// Right side produces group, team, and org subjects. None match user.
			left := dsIter("document", "parent", "folder", "...")
			rightGroup := dsIter("folder", "viewer", "group", "...")
			rightTeam := dsIter("folder", "viewer", "team", "...")
			rightOrg := dsIter("folder", "viewer", "org", "...")
			right := NewUnionIterator(rightGroup, rightTeam, rightOrg)
			arrow := NewIntersectionArrowIterator(left, right)

			result, changed, err := ApplyReachabilityPruning(arrow, "user")
			require.NoError(t, err)
			require.True(t, changed)
			require.IsType(t, &FixedIterator{}, result, "entire intersection arrow should be pruned")
		})

		t.Run("prunes intersection arrow inside union when unreachable", func(t *testing.T) {
			t.Parallel()

			// Union[
			//   document#viewer->user,
			//   IntersectionArrow[document#parent->folder, folder#viewer->group]
			// ]
			// The intersection arrow's right side produces group, not user, so it should get pruned.
			directUser := dsIter("document", "viewer", "user", "...")
			left := dsIter("document", "parent", "folder", "...")
			right := dsIter("folder", "viewer", "group", "...")
			arrow := NewIntersectionArrowIterator(left, right)
			union := NewUnionIterator(directUser, arrow)

			result, changed, err := ApplyReachabilityPruning(union, "user")
			require.NoError(t, err)
			require.True(t, changed)

			require.IsType(t, &UnionIterator{}, result)
			subs := result.Subiterators()
			require.Len(t, subs, 2)
			require.IsType(t, &DatastoreIterator{}, subs[0], "direct user branch should remain")
			require.IsType(t, &FixedIterator{}, subs[1], "intersection arrow branch should be pruned")
		})
	})
}
