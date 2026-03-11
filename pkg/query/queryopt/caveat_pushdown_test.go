package queryopt

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// dsOutline returns a DatastoreIteratorType outline whose BaseRelation carries
// the given caveat name (empty string means no caveat).
func dsOutline(caveatName string) query.Outline {
	rel := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", "", caveatName, false)
	return query.Outline{
		Type: query.DatastoreIteratorType,
		Args: &query.IteratorArgs{Relation: rel},
	}
}

// caveatArgs returns an IteratorArgs carrying the named caveat.
func caveatArgs(name string) *query.IteratorArgs {
	return &query.IteratorArgs{
		Caveat: &core.ContextualizedCaveat{CaveatName: name},
	}
}

// caveatOutline wraps child in a CaveatIteratorType outline for the named caveat.
func caveatOutline(name string, child query.Outline) query.Outline {
	return query.Outline{
		Type:        query.CaveatIteratorType,
		Args:        caveatArgs(name),
		SubOutlines: []query.Outline{child},
	}
}

// unionOutline returns a UnionIteratorType outline with the given children.
func unionOutline(children ...query.Outline) query.Outline {
	return query.Outline{
		Type:        query.UnionIteratorType,
		SubOutlines: children,
	}
}

// intersectionOutline returns an IntersectionIteratorType outline with the given children.
func intersectionOutline(children ...query.Outline) query.Outline {
	return query.Outline{
		Type:        query.IntersectionIteratorType,
		SubOutlines: children,
	}
}

// intersectionArrowOutline returns an IntersectionArrowIteratorType outline with left/right children.
func intersectionArrowOutline(left, right query.Outline) query.Outline {
	return query.Outline{
		Type:        query.IntersectionArrowIteratorType,
		SubOutlines: []query.Outline{left, right},
	}
}

// applyPushdown runs caveatPushdown bottom-up over outline via MutateOutline.
func applyPushdown(outline query.Outline) query.Outline {
	return query.MutateOutline(outline, []query.OutlineMutation{caveatPushdown})
}

func TestCaveatPushdown(t *testing.T) {
	t.Parallel()

	t.Run("pushes caveat through union when both sides have caveat", func(t *testing.T) {
		t.Parallel()

		// Caveat(Union[DS(cav), DS(cav)])
		input := caveatOutline("test_caveat", unionOutline(
			dsOutline("test_caveat"),
			dsOutline("test_caveat"),
		))

		result := applyPushdown(input)

		// → Union[Caveat(DS(cav)), Caveat(DS(cav))]
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("pushes caveat through union only on side with caveat", func(t *testing.T) {
		t.Parallel()

		// Caveat(Union[DS(cav), DS(no cav)])
		input := caveatOutline("test_caveat", unionOutline(
			dsOutline("test_caveat"),
			dsOutline(""),
		))

		result := applyPushdown(input)

		// → Union[Caveat(DS(cav)), DS(no cav)]
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, "test_caveat", result.SubOutlines[0].Args.Caveat.CaveatName)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("does not push caveat through intersection arrow", func(t *testing.T) {
		t.Parallel()

		// Caveat(IntersectionArrow[DS(cav), DS(no cav)])
		input := caveatOutline("test_caveat", intersectionArrowOutline(
			dsOutline("test_caveat"),
			dsOutline(""),
		))

		result := applyPushdown(input)

		// → unchanged: Caveat(IntersectionArrow[...])
		require.Equal(t, query.CaveatIteratorType, result.Type)
		require.Equal(t, query.IntersectionArrowIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("does not push when no children contain the caveat", func(t *testing.T) {
		t.Parallel()

		// Caveat(Union[DS(no cav), DS(no cav)])
		input := caveatOutline("test_caveat", unionOutline(
			dsOutline(""),
			dsOutline(""),
		))

		result := applyPushdown(input)

		// → unchanged: Caveat(Union[...])
		require.Equal(t, query.CaveatIteratorType, result.Type)
		require.Equal(t, query.UnionIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("does not push through leaf datastore node", func(t *testing.T) {
		t.Parallel()

		// Caveat(DS(cav)) — DS has no SubOutlines, nothing to push into
		input := caveatOutline("test_caveat", dsOutline("test_caveat"))

		result := applyPushdown(input)

		// → unchanged: Caveat(DS(cav))
		require.Equal(t, query.CaveatIteratorType, result.Type)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("pushes all the way through nested union recursively", func(t *testing.T) {
		t.Parallel()

		// Three levels deep: Caveat(Union[Union[Union[DS(cav), DS(no cav)], DS(no cav)], DS(cav)])
		//
		// MutateOutline walks bottom-up, so by the time the outer Caveat node is
		// visited, all three inner Union nodes have already been processed without
		// a caveat. The recursive call inside caveatPushdown is what drives the
		// caveat all the way down through the two intermediate Union levels to reach
		// the leaf DS(cav). Without it, pushdown would stop one level short.
		input := caveatOutline("test_caveat", unionOutline(
			unionOutline(
				unionOutline(
					dsOutline("test_caveat"),
					dsOutline(""),
				),
				dsOutline(""),
			),
			dsOutline("test_caveat"),
		))

		result := applyPushdown(input)

		// → Union[Union[Union[Caveat(DS(cav)), DS(no cav)], DS(no cav)], Caveat(DS(cav))]
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)

		// left branch: the caveat must be pushed all three levels deep
		mid := result.SubOutlines[0]
		require.Equal(t, query.UnionIteratorType, mid.Type)
		require.Len(t, mid.SubOutlines, 2)
		require.Equal(t, query.DatastoreIteratorType, mid.SubOutlines[1].Type) // DS(no cav) unchanged

		inner := mid.SubOutlines[0]
		require.Equal(t, query.UnionIteratorType, inner.Type)
		require.Len(t, inner.SubOutlines, 2)
		require.Equal(t, query.CaveatIteratorType, inner.SubOutlines[0].Type)    // Caveat(DS(cav)) at the leaf
		require.Equal(t, query.DatastoreIteratorType, inner.SubOutlines[1].Type) // DS(no cav) unchanged

		// right branch: leaf DS wrapped directly
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[1].Type)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].SubOutlines[0].Type)
	})

	t.Run("works with intersection of relations", func(t *testing.T) {
		t.Parallel()

		// Caveat(Intersection[DS(cav), DS(no cav)])
		input := caveatOutline("test_caveat", intersectionOutline(
			dsOutline("test_caveat"),
			dsOutline(""),
		))

		result := applyPushdown(input)

		// → Intersection[Caveat(DS(cav)), DS(no cav)]
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("does not push through nested caveat node", func(t *testing.T) {
		t.Parallel()

		// Caveat(Caveat(DS(cav)))
		// Bottom-up: inner Caveat(DS) is visited first — leaf below, no push.
		// Then outer Caveat sees a CaveatIteratorType child — blocked.
		input := caveatOutline("test_caveat", caveatOutline("test_caveat", dsOutline("test_caveat")))

		result := applyPushdown(input)

		// → unchanged: Caveat(Caveat(DS))
		require.Equal(t, query.CaveatIteratorType, result.Type)
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].SubOutlines[0].Type)
	})
}

func TestOutlineContainsCaveat(t *testing.T) {
	t.Parallel()

	t.Run("detects caveat directly in datastore node", func(t *testing.T) {
		t.Parallel()

		// Pushdown is blocked at leaf, so result stays Caveat(DS) — proving the
		// caveat was detected (otherwise nothing would have changed to check).
		// Test containment indirectly: wrap in union with a no-caveat peer.
		input := caveatOutline("test_caveat", unionOutline(dsOutline("test_caveat"), dsOutline("")))
		result := applyPushdown(input)
		// The caveat-bearing branch got wrapped, proving detection worked.
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Equal(t, query.CaveatIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("does not detect when caveat name differs", func(t *testing.T) {
		t.Parallel()

		// Caveat("test_caveat") over a DS with "other_caveat" — no match
		input := caveatOutline("test_caveat", unionOutline(
			dsOutline("other_caveat"),
			dsOutline(""),
		))
		result := applyPushdown(input)

		// Neither child contains "test_caveat", so nothing is pushed
		require.Equal(t, query.CaveatIteratorType, result.Type)
	})

	t.Run("detects caveat in nested datastore", func(t *testing.T) {
		t.Parallel()

		// Caveat(Union[DS(no cav), Union[DS(no cav), DS(cav)]])
		input := caveatOutline("test_caveat", unionOutline(
			dsOutline(""),
			unionOutline(dsOutline(""), dsOutline("test_caveat")),
		))
		result := applyPushdown(input)

		// First child has no caveat anywhere → left unwrapped.
		// Second child contains the caveat deep → wrapped and recursively pushed.
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)

		// The second branch should be a union with the caveat pushed all the way down
		innerResult := result.SubOutlines[1]
		require.Equal(t, query.UnionIteratorType, innerResult.Type)
		require.Equal(t, query.DatastoreIteratorType, innerResult.SubOutlines[0].Type)
		require.Equal(t, query.CaveatIteratorType, innerResult.SubOutlines[1].Type)
	})
}
