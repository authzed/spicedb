package queryopt

import (
	"slices"

	"github.com/authzed/spicedb/pkg/query"
)

// ApplyReachabilityPruning applies subject-type reachability pruning to a
// CanonicalOutline, replacing subtrees with NullIteratorType nodes when they
// can never produce the target subject type.
//
// For arrows (e.g. editor->view), the pruning decision is based on whether the
// right side (computed userset) can reach the target subject type. If not, the
// entire arrow is elided. (The left side's subject types are intermediate hops
// and are not considered for pruning.)
//
// For exclusions (e.g. editor - writer), the pruning decision is based on whether the
// main set on the left can reach the target subject type. The right set is ignored.
//
// For intersections (e.g. a & b & c), the pruning decision is based on whether
// all of the branches can reach the target subject type. If at least one cannot, the
// entire intersection is elided.
func ApplyReachabilityPruning(co query.CanonicalOutline, targetSubjectType string) query.CanonicalOutline {
	pruned, _ := pruneOutline(co.Root, targetSubjectType)

	// TODO do i need to call FillMissingIds?
	co.Root = pruned
	return co
}

// pruneOutline recursively prunes an outline tree, replacing subtrees that
// cannot produce the target subject type with NullIteratorType nodes.
// Returns the (possibly replaced) outline and whether any change was made.
func pruneOutline(outline query.Outline, targetSubjectType string) (query.Outline, bool) {
	// Recurse into all children first (bottom-up).
	changed := false
	if len(outline.SubOutlines) > 0 {
		newSubs := make([]query.Outline, len(outline.SubOutlines))
		copy(newSubs, outline.SubOutlines)

		for i, sub := range newSubs {
			newSub, ok := pruneOutline(sub, targetSubjectType)
			if ok {
				newSubs[i] = newSub
				changed = true
			}
		}
		if changed {
			outline = query.Outline{
				Type:        outline.Type,
				Args:        outline.Args,
				SubOutlines: newSubs,
				ID:          outline.ID,
			}
		}
	}

	// Now check this node's subject types.
	if slices.Contains(outlineSubjectTypes(outline), targetSubjectType) {
		return outline, changed
	}

	return query.Outline{Type: query.NullIteratorType, ID: outline.ID}, true
}

// outlineSubjectTypes computes the subject types reachable from an outline
// node, mirroring the Iterator.SubjectTypes() logic for each node type.
func outlineSubjectTypes(outline query.Outline) []string {
	switch outline.Type {
	case query.DatastoreIteratorType:
		if outline.Args != nil && outline.Args.Relation != nil {
			return []string{outline.Args.Relation.Type()}
		}
		return nil

	case query.ArrowIteratorType, query.IntersectionArrowIteratorType:
		// Subject types come from the right child only.
		if len(outline.SubOutlines) == 2 {
			return outlineSubjectTypes(outline.SubOutlines[1])
		}
		return nil

	case query.ExclusionIteratorType:
		// Subject types come from left child only.
		if len(outline.SubOutlines) == 2 {
			return outlineSubjectTypes(outline.SubOutlines[0])
		}
		return nil

	case query.UnionIteratorType:
		// Subject types come from all children.
		var types []string
		for _, sub := range outline.SubOutlines {
			types = append(types, outlineSubjectTypes(sub)...)
		}
		return types

	case query.IntersectionIteratorType:
		// All branches must produce results, so if any branch has no
		// reachable subject types, the intersection can never produce results.
		var types []string
		for _, sub := range outline.SubOutlines {
			childTypes := outlineSubjectTypes(sub)
			if len(childTypes) == 0 {
				return nil
			}
			types = append(types, childTypes...)
		}
		return types

	case query.SelfIteratorType:
		if outline.Args != nil && outline.Args.DefinitionName != "" {
			return []string{outline.Args.DefinitionName}
		}
		return nil

	case query.CaveatIteratorType, query.AliasIteratorType, query.RecursiveIteratorType:
		if len(outline.SubOutlines) == 1 {
			return outlineSubjectTypes(outline.SubOutlines[0])
		}
		return nil

	default:
		// FixedIteratorType, NullIteratorType, RecursiveSentinelIteratorType
		// TODO is this correct?
		return nil
	}
}
