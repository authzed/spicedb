package query

import (
	"slices"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CanonicalizeOutline transforms an Outline into canonical form.
// Canonicalization standardizes the representation of logically equivalent queries
// to enable efficient comparison and deduplication.
//
// The canonicalization process has two phases:
// 1. Filter Lifting: Extract all caveats, sort them, and nest them at the top
// 2. Bottom-Up Canonicalization: Apply five transformation steps sequentially
//
// The function is idempotent: applying it multiple times produces the same result.
func CanonicalizeOutline(outline Outline) (Outline, error) {
	// Phase 1: Extract and lift caveats
	caveatlessTree, caveats := extractAndLiftCaveats(outline)

	// Phase 2: Apply bottom-up canonicalization
	canonicalTree := applyBottomUpCanonicalization(caveatlessTree)

	// Wrap with caveats
	result := nestCaveats(canonicalTree, caveats)

	// Populate CanonicalKey fields bottom-up
	result = populateCanonicalKeys(result)

	return result, nil
}

// extractAndLiftCaveats extracts all caveats from the tree, sorts them,
// and returns the caveatless tree and sorted caveat list.
func extractAndLiftCaveats(outline Outline) (Outline, []*core.ContextualizedCaveat) {
	caveatlessTree, caveats := extractCaveats(outline)

	// Sort caveats for deterministic ordering
	slices.SortFunc(caveats, caveatCompare)

	return caveatlessTree, caveats
}

// extractCaveats recursively extracts all CaveatIteratorType nodes from the tree.
// Returns the tree with caveats removed and a list of all extracted caveats.
func extractCaveats(outline Outline) (Outline, []*core.ContextualizedCaveat) {
	// If this is a caveat node, extract it
	if outline.Type == CaveatIteratorType {
		if outline.Args == nil || outline.Args.Caveat == nil {
			// Malformed caveat node, skip it
			if len(outline.Subiterators) > 0 {
				return extractCaveats(outline.Subiterators[0])
			}
			return Outline{Type: NullIteratorType}, nil
		}

		// Extract the caveat and recurse on the child
		caveat := outline.Args.Caveat
		var childTree Outline
		var childCaveats []*core.ContextualizedCaveat

		if len(outline.Subiterators) > 0 {
			childTree, childCaveats = extractCaveats(outline.Subiterators[0])
		} else {
			childTree = Outline{Type: NullIteratorType}
		}

		// Return the child tree and all caveats (innermost caveats first)
		return childTree, append(childCaveats, caveat)
	}

	// Not a caveat node, recurse on all subiterators
	if len(outline.Subiterators) == 0 {
		return outline, nil
	}

	newSubs := make([]Outline, len(outline.Subiterators))
	var allCaveats []*core.ContextualizedCaveat

	for i, sub := range outline.Subiterators {
		newSub, subCaveats := extractCaveats(sub)
		newSubs[i] = newSub
		allCaveats = append(allCaveats, subCaveats...)
	}

	return Outline{
		Type:         outline.Type,
		Args:         outline.Args,
		Subiterators: newSubs,
	}, allCaveats
}

// nestCaveats wraps the given outline in nested CaveatIteratorType nodes,
// one for each caveat in the list. The caveats are nested in reverse order
// so that the first caveat in the list becomes the outermost caveat.
func nestCaveats(outline Outline, caveats []*core.ContextualizedCaveat) Outline {
	result := outline

	// Nest in reverse order so first caveat becomes outermost
	for i := len(caveats) - 1; i >= 0; i-- {
		result = Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{
				Caveat: caveats[i],
			},
			Subiterators: []Outline{result},
		}
	}

	return result
}

// applyBottomUpCanonicalization applies five canonicalization steps sequentially
// in a single bottom-up traversal of the tree.
// Some steps are applied multiple times to ensure convergence (e.g., collapseSingleChild
// after propagateNull, since propagateNull can create single-child composites).
func applyBottomUpCanonicalization(outline Outline) Outline {
	steps := []OutlineMutation{
		replaceEmptyComposites,
		propagateNull,
		flattenComposites,
		collapseSingleChild,
		sortCompositeChildren,
	}
	return MutateOutline(outline, steps)
}

// replaceEmptyComposites converts Union/Intersection with 0 children to NullIteratorType.
func replaceEmptyComposites(outline Outline) Outline {
	if (outline.Type == UnionIteratorType || outline.Type == IntersectionIteratorType) &&
		len(outline.Subiterators) == 0 {
		return Outline{Type: NullIteratorType}
	}
	return outline
}

// collapseSingleChild removes unnecessary Union/Intersection wrappers
// that contain only a single subiterator.
func collapseSingleChild(outline Outline) Outline {
	if (outline.Type == UnionIteratorType || outline.Type == IntersectionIteratorType) &&
		len(outline.Subiterators) == 1 {
		return outline.Subiterators[0]
	}
	return outline
}

// propagateNull handles null propagation rules:
// - Intersection with any null child becomes null
// - Union with all null children becomes null
// - Union with some null children removes the nulls
func propagateNull(outline Outline) Outline {
	switch outline.Type {
	case IntersectionIteratorType:
		// Any null child makes the intersection null
		if slices.ContainsFunc(outline.Subiterators, isNullOutline) {
			return Outline{Type: NullIteratorType}
		}
	case UnionIteratorType:
		// Filter out null children
		nonNullSubs := make([]Outline, 0, len(outline.Subiterators))
		for _, sub := range outline.Subiterators {
			if !isNullOutline(sub) {
				nonNullSubs = append(nonNullSubs, sub)
			}
		}

		// If all children were null, return null
		if len(nonNullSubs) == 0 {
			return Outline{Type: NullIteratorType}
		}

		// If some children were null, return union of non-null children
		if len(nonNullSubs) < len(outline.Subiterators) {
			return Outline{
				Type:         UnionIteratorType,
				Args:         outline.Args,
				Subiterators: nonNullSubs,
			}
		}
	}
	return outline
}

// flattenComposites flattens nested composites of the same type.
// Union[Union[A,B],C] becomes Union[A,B,C]
// Intersection[Intersection[A,B],C] becomes Intersection[A,B,C]
func flattenComposites(outline Outline) Outline {
	if outline.Type != UnionIteratorType && outline.Type != IntersectionIteratorType {
		return outline
	}

	// Check if any children match the parent type
	needsFlattening := false
	for _, sub := range outline.Subiterators {
		if sub.Type == outline.Type {
			needsFlattening = true
			break
		}
	}

	if !needsFlattening {
		return outline
	}

	// Flatten: collect all grandchildren from matching children
	flattenedSubs := make([]Outline, 0, len(outline.Subiterators))
	for _, sub := range outline.Subiterators {
		if sub.Type == outline.Type {
			// Child matches parent type, append its children
			flattenedSubs = append(flattenedSubs, sub.Subiterators...)
		} else {
			// Child doesn't match, keep it as is
			flattenedSubs = append(flattenedSubs, sub)
		}
	}

	return Outline{
		Type:         outline.Type,
		Args:         outline.Args,
		Subiterators: flattenedSubs,
	}
}

// sortCompositeChildren sorts the children of Union/Intersection
// using OutlineCompare for deterministic ordering.
func sortCompositeChildren(outline Outline) Outline {
	if (outline.Type == UnionIteratorType || outline.Type == IntersectionIteratorType) &&
		len(outline.Subiterators) > 1 {
		sortedSubs := make([]Outline, len(outline.Subiterators))
		copy(sortedSubs, outline.Subiterators)
		slices.SortFunc(sortedSubs, OutlineCompare)

		return Outline{
			Type:         outline.Type,
			Args:         outline.Args,
			Subiterators: sortedSubs,
		}
	}
	return outline
}

// isNullOutline returns true if the outline represents a null/empty iterator.
// This includes NullIteratorType and FixedIteratorType with no paths.
func isNullOutline(outline Outline) bool {
	if outline.Type == NullIteratorType {
		return true
	}
	if outline.Type == FixedIteratorType {
		if outline.Args == nil || len(outline.Args.FixedPaths) == 0 {
			return true
		}
	}
	return false
}

// populateCanonicalKeys recursively sets CanonicalKey on all nodes
// after canonicalization completes. Must be called bottom-up.
func populateCanonicalKeys(outline Outline) Outline {
	// Recurse on children first
	if len(outline.Subiterators) > 0 {
		newSubs := make([]Outline, len(outline.Subiterators))
		for i, sub := range outline.Subiterators {
			newSubs[i] = populateCanonicalKeys(sub)
		}
		outline.Subiterators = newSubs
	}

	// Set CanonicalKey on this node
	outline.CanonicalKey = SerializeOutline(outline)
	return outline
}
