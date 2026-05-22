package queryopt

import (
	"github.com/authzed/spicedb/pkg/query"
)

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "alias-chain-collapse",
		Description: `
		Collapses a chain of AliasIteratorType nodes into a single alias iterator.
		The collapsed node keeps the innermost (most-leaf) alias's RelationName
		as its underlying identity (used for the canonical/cache key and the
		relation read from the datastore) and records every outer name in the
		chain — in inner-to-outer order — as AliasedAs. The outermost name
		(last entry) becomes the user-facing relation written onto emitted
		paths; the self-edge fires for any subject relation in the chain
		(RelationName ∪ AliasedAs), preserving the multi-level self-edge
		semantics of the uncollapsed chain.
		`,
		NewTransform: func(_ RequestParams) OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(outline, []query.OutlineMutation{collapseAliasChain})
			}
		},
	})
}

// collapseAliasChain is an OutlineMutation that collapses a chain of
// AliasIteratorType nodes into a single alias iterator.
//
// It is called bottom-up by MutateOutline, so by the time an AliasIteratorType
// node is visited, its child has already been processed. If the child is also
// an AliasIteratorType, the two are collapsed: the inner's RelationName is
// kept as the merged identity, and the outer's RelationName is appended to
// the inner's AliasedAs. The order of AliasedAs is inner-to-outer, so the
// last entry is always the outermost name in the original chain.
//
// In a chain of three or more aliases, each upward visit appends the next
// outer name onto the slice produced by the previous collapse.
//
// Example (left-to-right is bottom-up):
//
//	Alias("viewer")(DS)                              kept as-is
//	Alias("view")(Alias("viewer")(DS))               → Alias{RelationName:"viewer", AliasedAs:["view"]}(DS)
//	Alias("perm")(prev)                              → Alias{RelationName:"viewer", AliasedAs:["view","perm"]}(DS)
func collapseAliasChain(outline query.Outline) query.Outline {
	if outline.Type != query.AliasIteratorType {
		return outline
	}

	if len(outline.SubOutlines) != 1 {
		return outline
	}

	if outline.Args == nil || outline.Args.RelationName == "" {
		return outline
	}

	child := outline.SubOutlines[0]
	if child.Type != query.AliasIteratorType || child.Args == nil {
		return outline
	}

	// Keep the inner identity; append the outer name to the chain.
	mergedArgs := *child.Args
	mergedArgs.AliasedAs = append(append([]string(nil), child.Args.AliasedAs...), outline.Args.RelationName)

	// Drop the ID so FillMissingNodeIDs reassigns and re-serializes with the
	// new args (the structure differs from the pre-collapse outline, so the
	// pre-collapse canonical key would be stale).
	return query.Outline{
		Type:        query.AliasIteratorType,
		Args:        &mergedArgs,
		SubOutlines: child.SubOutlines,
	}
}
