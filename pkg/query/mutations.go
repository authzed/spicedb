package query

// OutlineMutation is a function that transforms an Outline node.
// Mutations are typically applied during a bottom-up tree traversal.
type OutlineMutation func(Outline) Outline

// MutateOutline performs a bottom-up traversal of the outline tree, applying
// all the given transformation functions to each node after processing its children.
func MutateOutline(outline Outline, fns []OutlineMutation) Outline {
	// Recurse on children first (bottom-up)
	if len(outline.SubOutlines) > 0 {
		newSubs := make([]Outline, len(outline.SubOutlines))
		for i, sub := range outline.SubOutlines {
			newSubs[i] = MutateOutline(sub, fns)
		}
		outline = Outline{
			Type:        outline.Type,
			Args:        outline.Args,
			SubOutlines: newSubs,
			ID:          outline.ID,
		}
	}

	// Then apply all mutation functions in sequence to current node
	result := outline
	for _, fn := range fns {
		result = fn(result)
	}
	return result
}

// ReorderMutation returns an OutlineMutation that reorders SubOutlines according
// to order, where order[i] is the index of the child to place at position i.
// If order has a different length than the node's SubOutlines, it is a no-op.
func ReorderMutation(order []int) OutlineMutation {
	return func(outline Outline) Outline {
		if len(order) != len(outline.SubOutlines) {
			return outline
		}
		reordered := make([]Outline, len(order))
		for i, idx := range order {
			reordered[i] = outline.SubOutlines[idx]
		}
		return Outline{
			Type:        outline.Type,
			Args:        outline.Args,
			SubOutlines: reordered,
			ID:          outline.ID,
		}
	}
}

// RotateArrowMutation returns an OutlineMutation that rotates nested arrow structures
// to potentially reduce execution cost.
//
// If rotateLeft is true:
//   - Transforms (A->B)->C into A->(B->C)
//   - Requires: outline is ArrowIteratorType with left child also ArrowIteratorType
//
// If rotateLeft is false:
//   - Transforms A->(B->C) into (A->B)->C
//   - Requires: outline is ArrowIteratorType with right child also ArrowIteratorType
//
// Returns outline unchanged if preconditions are not met or if the outline
// is not an arrow type.
func RotateArrowMutation(rotateLeft bool) OutlineMutation {
	return func(outline Outline) Outline {
		// Must be an arrow with exactly 2 children
		if outline.Type != ArrowIteratorType || len(outline.SubOutlines) != 2 {
			return outline
		}

		left := outline.SubOutlines[0]
		right := outline.SubOutlines[1]

		if rotateLeft {
			// Transform (A->B)->C into A->(B->C)
			// Requires left child to be an arrow
			if left.Type != ArrowIteratorType || len(left.SubOutlines) != 2 {
				return outline
			}

			// Left child structure: A->B
			a := left.SubOutlines[0]
			b := left.SubOutlines[1]
			c := right

			// Build B->C (inner arrow on right)
			innerArrow := Outline{
				Type:        ArrowIteratorType,
				Args:        outline.Args, // Preserve Args from parent
				SubOutlines: []Outline{b, c},
			}

			// Build A->(B->C)
			return Outline{
				Type:        ArrowIteratorType,
				Args:        outline.Args, // Preserve Args from parent
				SubOutlines: []Outline{a, innerArrow},
				ID:          outline.ID,
			}
		}

		// Transform A->(B->C) into (A->B)->C
		// Requires right child to be an arrow
		if right.Type != ArrowIteratorType || len(right.SubOutlines) != 2 {
			return outline
		}

		// Right child structure: B->C
		a := left
		b := right.SubOutlines[0]
		c := right.SubOutlines[1]

		// Build A->B (inner arrow on left)
		innerArrow := Outline{
			Type:        ArrowIteratorType,
			Args:        outline.Args, // Preserve Args from parent
			SubOutlines: []Outline{a, b},
		}

		// Build (A->B)->C
		return Outline{
			Type:        ArrowIteratorType,
			Args:        outline.Args, // Preserve Args from parent
			SubOutlines: []Outline{innerArrow, c},
			ID:          outline.ID,
		}
	}
}
