package queryopt

import "github.com/authzed/spicedb/pkg/query"

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "simple-caveat-pushdown",
		Description: `
		Pushes caveat evalution to the lowest point in the tree.
		Cannot push through intersection arrows
		`,
		Mutation: caveatPushdown,
	})
}

// caveatPushdown is an OutlineMutation that implements caveat pushdown on Outline trees.
// It is called bottom-up by MutateOutline, so by the time a CaveatIteratorType node is
// visited, its children have already been processed.
//
// For a node  Caveat(child)  it attempts to push the caveat one level deeper:
//
//	Caveat(Union[A, B])        → Union[Caveat(A), B]         (only A contains the caveat)
//	Caveat(Union[A, B])        → Union[Caveat(A), Caveat(B)] (both contain the caveat)
//
// Pushdown is blocked (outline returned unchanged) when:
//   - the node is not a CaveatIteratorType
//   - the child is an IntersectionArrowIteratorType (special all() semantics)
//   - the child is another CaveatIteratorType (would cause infinite recursion)
//   - the child has no SubOutlines (leaf node)
//   - none of the child's SubOutlines contain the caveat
//
// The original caveat node's ID is threaded through to all newly created caveat
// wrappers. Because CaveatIterator nodes serialize solely by caveat name (independent
// of position), all relocated instances share the same CanonicalKey and therefore
// legitimately carry the same ID.
func caveatPushdown(outline query.Outline) query.Outline {
	if outline.Type != query.CaveatIteratorType {
		return outline
	}
	return caveatPushdownInner(outline, outline.ID)
}

// caveatPushdownInner is the recursive implementation of caveatPushdown.
// originalID is the ID of the topmost caveat node being pushed; it is threaded
// through to every caveat wrapper created during the descent so that all
// relocated instances of the same caveat carry the original ID.
func caveatPushdownInner(outline query.Outline, originalID query.OutlineNodeID) query.Outline {
	// A CaveatIterator must have exactly one child.
	if len(outline.SubOutlines) != 1 {
		return outline
	}
	child := outline.SubOutlines[0]

	// Do not push through IntersectionArrow (all() semantics require post-intersection caveat eval).
	if child.Type == query.IntersectionArrowIteratorType {
		return outline
	}

	// Do not push through another CaveatIterator (prevents infinite recursion).
	if child.Type == query.CaveatIteratorType {
		return outline
	}

	// Nothing to push into if the child has no children of its own.
	if len(child.SubOutlines) == 0 {
		return outline
	}

	caveat := outline.Args.Caveat

	// For each grandchild, wrap it in the caveat if it contains the caveat relation,
	// then recursively push the new caveat wrapper as deep as it will go.
	// The originalID is threaded through so every new caveat wrapper carries the
	// same ID as the caveat node being relocated.
	newSubs := make([]query.Outline, len(child.SubOutlines))
	changed := false
	for i, sub := range child.SubOutlines {
		if outlineContainsCaveat(sub, caveat.CaveatName) {
			wrapped := query.Outline{
				Type:        query.CaveatIteratorType,
				Args:        outline.Args,
				SubOutlines: []query.Outline{sub},
				ID:          originalID,
			}
			newSubs[i] = caveatPushdownInner(wrapped, originalID)
			changed = true
		} else {
			newSubs[i] = sub
		}
	}

	if !changed {
		return outline
	}

	// Return the child with updated grandchildren, dropping the outer caveat wrapper.
	return query.Outline{
		Type:        child.Type,
		Args:        child.Args,
		SubOutlines: newSubs,
		ID:          child.ID,
	}
}

// outlineContainsCaveat reports whether the outline subtree contains any
// DatastoreIteratorType node whose Relation carries the given caveat name.
func outlineContainsCaveat(outline query.Outline, caveatName string) bool {
	if outline.Type == query.DatastoreIteratorType {
		if outline.Args != nil && outline.Args.Relation != nil {
			if outline.Args.Relation.Caveat() == caveatName {
				return true
			}
		}
	}
	for _, sub := range outline.SubOutlines {
		if outlineContainsCaveat(sub, caveatName) {
			return true
		}
	}
	return false
}
