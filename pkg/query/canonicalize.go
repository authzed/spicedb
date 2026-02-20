package query

import (
	"slices"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
	caveatlessTree, caveats, err := extractCaveats(outline)
	if err != nil {
		return Outline{}, err
	}

	// Sort caveats for deterministic ordering
	slices.SortFunc(caveats, caveatCompare)

	// Phase 2: Apply bottom-up canonicalization
	canonicalTree := applyBottomUpCanonicalization(caveatlessTree)

	// Wrap with caveats
	result := nestCaveats(canonicalTree, caveats)

	// Populate CanonicalKey fields bottom-up
	result = populateCanonicalKeys(result)

	return result, nil
}

// extractCaveats recursively extracts all CaveatIteratorType nodes from the tree.
// Returns the tree with caveats removed and a list of all extracted caveats.
func extractCaveats(outline Outline) (Outline, []*core.ContextualizedCaveat, error) {
	// If this is a caveat node, extract it
	if outline.Type == CaveatIteratorType {
		if outline.Args == nil || outline.Args.Caveat == nil {
			// Malformed caveat node?
			return Outline{}, nil, spiceerrors.MustBugf("extractCaveats: malformed caveat node")
		}

		// Extract the caveat and recurse on the child
		caveat := outline.Args.Caveat
		var err error
		var childTree Outline
		var childCaveats []*core.ContextualizedCaveat

		if len(outline.SubOutlines) > 0 {
			childTree, childCaveats, err = extractCaveats(outline.SubOutlines[0])
		} else {
			childTree = Outline{Type: NullIteratorType}
		}

		// Return the child tree and all caveats (innermost caveats first)
		return childTree, append(childCaveats, caveat), err
	}

	// Not a caveat node, recurse on all subiterators
	if len(outline.SubOutlines) == 0 {
		return outline, nil, nil
	}

	newSubs := make([]Outline, len(outline.SubOutlines))
	var allCaveats []*core.ContextualizedCaveat

	for i, sub := range outline.SubOutlines {
		newSub, subCaveats, err := extractCaveats(sub)
		if err != nil {
			return outline, nil, err
		}
		newSubs[i] = newSub
		allCaveats = append(allCaveats, subCaveats...)
	}

	return Outline{
		Type:        outline.Type,
		Args:        outline.Args,
		SubOutlines: newSubs,
	}, allCaveats, nil
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
			SubOutlines: []Outline{result},
		}
	}

	return result
}

var canonicalizationSteps = []OutlineMutation{
	replaceEmptyComposites,
	propagateNull,
	flattenComposites,
	collapseSingleChild,
	sortCompositeChildren,
}

// applyBottomUpCanonicalization applies five canonicalization steps sequentially
// in a single bottom-up traversal of the tree.
func applyBottomUpCanonicalization(outline Outline) Outline {
	return MutateOutline(outline, canonicalizationSteps)
}

// replaceEmptyComposites converts Union/Intersection with 0 children to NullIteratorType.
func replaceEmptyComposites(outline Outline) Outline {
	if (outline.Type == UnionIteratorType || outline.Type == IntersectionIteratorType) &&
		len(outline.SubOutlines) == 0 {
		return Outline{Type: NullIteratorType}
	}
	return outline
}

// collapseSingleChild removes unnecessary Union/Intersection wrappers
// that contain only a single subiterator.
func collapseSingleChild(outline Outline) Outline {
	if (outline.Type == UnionIteratorType || outline.Type == IntersectionIteratorType) &&
		len(outline.SubOutlines) == 1 {
		return outline.SubOutlines[0]
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
		if slices.ContainsFunc(outline.SubOutlines, isNullOutline) {
			return Outline{Type: NullIteratorType}
		}
	case UnionIteratorType:
		// Filter out null children
		nonNullSubs := make([]Outline, 0, len(outline.SubOutlines))
		for _, sub := range outline.SubOutlines {
			if !isNullOutline(sub) {
				nonNullSubs = append(nonNullSubs, sub)
			}
		}

		// If all children were null, return null
		if len(nonNullSubs) == 0 {
			return Outline{Type: NullIteratorType}
		}

		// If some children were null, return union of non-null children
		if len(nonNullSubs) < len(outline.SubOutlines) {
			return Outline{
				Type:        UnionIteratorType,
				Args:        outline.Args,
				SubOutlines: nonNullSubs,
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
	for _, sub := range outline.SubOutlines {
		if sub.Type == outline.Type {
			needsFlattening = true
			break
		}
	}

	if !needsFlattening {
		return outline
	}

	// Flatten: collect all grandchildren from matching children
	flattenedSubs := make([]Outline, 0, len(outline.SubOutlines))
	for _, sub := range outline.SubOutlines {
		if sub.Type == outline.Type {
			// Child matches parent type, append its children
			flattenedSubs = append(flattenedSubs, sub.SubOutlines...)
		} else {
			// Child doesn't match, keep it as is
			flattenedSubs = append(flattenedSubs, sub)
		}
	}

	return Outline{
		Type:        outline.Type,
		Args:        outline.Args,
		SubOutlines: flattenedSubs,
	}
}

// sortCompositeChildren sorts the children of Union/Intersection
// using OutlineCompare for deterministic ordering.
func sortCompositeChildren(outline Outline) Outline {
	if (outline.Type == UnionIteratorType || outline.Type == IntersectionIteratorType) &&
		len(outline.SubOutlines) > 1 {
		sortedSubs := make([]Outline, len(outline.SubOutlines))
		copy(sortedSubs, outline.SubOutlines)
		slices.SortFunc(sortedSubs, OutlineCompare)

		return Outline{
			Type:        outline.Type,
			Args:        outline.Args,
			SubOutlines: sortedSubs,
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
	if len(outline.SubOutlines) > 0 {
		newSubs := make([]Outline, len(outline.SubOutlines))
		for i, sub := range outline.SubOutlines {
			newSubs[i] = populateCanonicalKeys(sub)
		}
		outline.SubOutlines = newSubs
	}

	// Set CanonicalKey on this node
	outline.CanonicalKey = outline.Serialize()
	return outline
}
