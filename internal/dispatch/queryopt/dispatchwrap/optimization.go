package dispatchwrap

import (
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
)

const Name = "dispatch-wrap-alias"

type recursiveKey struct{ definition, relation string }

// New returns a dispatch-boundary optimization using the supplied iterator type.
func New(iteratorType query.IteratorType) optimization.Optimizer {
	return optimization.Optimizer{
		Name: Name,
		Description: `
		Wraps eligible aliases and complete recursive controllers in a dispatch passthrough
		so the dispatch layer can recognize the (definition, relation) boundary
		as a dispatch candidate without re-deriving it at runtime. Aliases whose
		subtree contains an unmatched RecursiveSentinel are left unwrapped — the
		sentinel must execute inside the collection context established by its
		matching RecursiveIterator, which is broken by an RPC hop. A complete
		recursive controller is safe to ship and gives the recursive relation
		ordinary result-cache coverage.
		`,
		NewTransform: func(_ optimization.RequestParams) optimization.OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(
					outline,
					[]query.OutlineMutation{func(node query.Outline) query.Outline { return wrapDispatchBoundary(node, iteratorType) }},
				)
			}
		},
	}
}

// wrapDispatchBoundary marks aliases and complete recursive controllers as
// relation boundaries. The controller includes the state required by its
// sentinels; wrapping only the alias inside its template would sever that state.
// This lets recursive relation results use the same cache as ordinary aliases.
//
// The remaining runtime guard (in-progress dispatch-key cycle break) cannot
// be made static and stays in the executor.
func wrapDispatchBoundary(outline query.Outline, iteratorType query.IteratorType) query.Outline {
	if outline.Type != query.AliasIteratorType && outline.Type != query.RecursiveIteratorType {
		return outline
	}
	if containsUnmatchedRecursiveSentinelOutline(outline) {
		return outline
	}
	// New node has ID==0; ApplyOptimizations runs FillMissingNodeIDs after the
	// transform, which assigns it a fresh ID and records its canonical key.
	return query.Outline{
		Type:        iteratorType,
		SubOutlines: []query.Outline{outline},
	}
}

// containsUnmatchedRecursiveSentinelOutline is the outline-time mirror of
// containsUnmatchedRecursiveSentinel (executor.go): it walks an outline
// subtree and reports whether any RecursiveSentinel has no RecursiveIterator
// with the same (definition, relation) key in the same subtree. An unmatched
// sentinel means dispatching this subtree would sever the sentinel from the
// collection context its iterator establishes.
func containsUnmatchedRecursiveSentinelOutline(root query.Outline) bool {
	var iteratorKeys map[recursiveKey]struct{}
	var sentinelKeys map[recursiveKey]struct{}

	var walk func(query.Outline)
	walk = func(node query.Outline) {
		switch node.Type {
		case query.RecursiveIteratorType:
			if node.Args != nil {
				if iteratorKeys == nil {
					iteratorKeys = make(map[recursiveKey]struct{})
				}
				iteratorKeys[recursiveKey{node.Args.DefinitionName, node.Args.RelationName}] = struct{}{}
			}
		case query.RecursiveSentinelIteratorType:
			if node.Args != nil {
				if sentinelKeys == nil {
					sentinelKeys = make(map[recursiveKey]struct{})
				}
				sentinelKeys[recursiveKey{node.Args.DefinitionName, node.Args.RelationName}] = struct{}{}
			}
		}
		for _, sub := range node.SubOutlines {
			walk(sub)
		}
	}
	walk(root)

	for k := range sentinelKeys {
		if _, managed := iteratorKeys[k]; !managed {
			return true
		}
	}
	return false
}
