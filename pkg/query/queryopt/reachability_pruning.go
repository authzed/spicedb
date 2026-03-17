package queryopt

import (
	"github.com/authzed/spicedb/pkg/query"
)

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "reachability-pruning",
		Description: `
		Replaces subtrees with NullIteratorType nodes when they can never
		produce the target subject type of the request.
		`,
		NewTransform: func(params RequestParams) OutlineTransform {
			return reachabilityPruning(params)
		},
	})
}

func reachabilityPruning(params RequestParams) func(outline query.Outline) query.Outline {
	return func(outline query.Outline) query.Outline {
		if params.SubjectType == "" || (params.SubjectRelation != "" && params.SubjectRelation != "...") {
			// do not mutate if subjectType is empty or if subjectRelation is non-empty
			return outline
		}
		// Pre-pass: find all node IDs inside arrow left subtrees.
		// These are intermediate hops whose subject type does not need
		// to match the target, so they must be excluded from pruning.
		immune := collectArrowLeftSubtreeIDs(outline)
		return query.MutateOutline(outline, []query.OutlineMutation{
			leafSubjectTypePruner(params.SubjectType, immune),
			query.NullPropagation,
		})
	}
}

// leafSubjectTypePruner returns an OutlineMutation that replaces leaf outline
// nodes (DatastoreIteratorType, SelfIteratorType) with NullIteratorType when
// their subject type does not match the target. Nodes whose IDs appear in the
// immune set are skipped — these are nodes inside arrow left subtrees whose
// subject types are intermediate hops, not final outputs.
//
// Null propagation through compound nodes (unions, intersections, arrows, etc.)
// is handled by query.NullPropagation, which should run after this mutation in
// the same MutateOutline call.
func leafSubjectTypePruner(targetSubjectType string, immune map[query.OutlineNodeID]bool) query.OutlineMutation {
	return func(outline query.Outline) query.Outline {
		if immune[outline.ID] {
			return outline
		}

		switch outline.Type {
		case query.DatastoreIteratorType:
			if outline.Args != nil && outline.Args.Relation != nil && outline.Args.Relation.Type() == targetSubjectType {
				return outline
			}
			return query.Outline{Type: query.NullIteratorType, ID: outline.ID}

		case query.SelfIteratorType:
			if outline.Args != nil && outline.Args.DefinitionName == targetSubjectType {
				return outline
			}
			return query.Outline{Type: query.NullIteratorType, ID: outline.ID}

		default:
			return outline
		}
	}
}

// collectArrowLeftSubtreeIDs walks the outline tree and collects the IDs of all
// nodes that appear inside the left subtree of an arrow (or intersection arrow).
// These nodes have intermediate subject types that should not be pruned.
func collectArrowLeftSubtreeIDs(outline query.Outline) map[query.OutlineNodeID]bool {
	ids := make(map[query.OutlineNodeID]bool)
	_ = query.WalkOutlinePreOrder(outline, func(node query.Outline) error {
		if (node.Type == query.ArrowIteratorType || node.Type == query.IntersectionArrowIteratorType) && len(node.SubOutlines) == 2 {
			markAllIDs(node.SubOutlines[0], ids)
		}
		return nil
	})
	return ids
}

// markAllIDs recursively adds the ID of every node in the subtree to the set.
func markAllIDs(outline query.Outline, ids map[query.OutlineNodeID]bool) {
	ids[outline.ID] = true
	for _, sub := range outline.SubOutlines {
		markAllIDs(sub, ids)
	}
}
