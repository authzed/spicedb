package queryopt

import (
	"github.com/authzed/spicedb/pkg/query"
)

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "alias-chain-collapse",
		Description: `
		Collapses a chain of AliasIteratorType nodes into a single alias iterator,
		using the relation name from the innermost (most-leaf) alias iterator.

		This optimization is safe for Check and LookupResources operations, where
		alias iterators do not need to emit self-edges for each intermediate alias.
		It is NOT safe for LookupSubjects, where each alias iterator may need to
		check for and prepend a self-edge based on whether the resource exists as
		a subject in the datastore.
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
// node is visited, its child has already been processed. If the child is also an
// AliasIteratorType, the two are collapsed into one using the child's (innermost)
// relation name.
//
// Example:
//
//	Alias("viewer")(Alias("owner")(DS)) → Alias("owner")(DS)
func collapseAliasChain(outline query.Outline) query.Outline {
	if outline.Type != query.AliasIteratorType {
		return outline
	}

	if len(outline.SubOutlines) != 1 {
		return outline
	}

	child := outline.SubOutlines[0]
	if child.Type != query.AliasIteratorType {
		return outline
	}

	// Collapse: replace the current (outer) alias with the already-collapsed
	// inner alias. The inner alias carries the relation name of the deepest
	// alias in the chain, which is the one we want to preserve.
	// Preserve the current node's ID so the canonical key mapping remains valid.
	return query.Outline{
		Type:        query.AliasIteratorType,
		Args:        child.Args,
		SubOutlines: child.SubOutlines,
		ID:          outline.ID,
	}
}
