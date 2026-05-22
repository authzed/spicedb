package dispatch

import (
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
)

// DispatchWrapAliasOptimizationName is the registry name for the
// dispatch-wrap-alias optimization. Callers fetch the registered Optimizer by
// this name when they want to apply the wrap as a standalone step after the
// usual queryopt.OptimizersForRequest set.
const DispatchWrapAliasOptimizationName = "dispatch-wrap-alias"

func init() {
	queryopt.MustRegisterOptimization(queryopt.Optimizer{
		Name: DispatchWrapAliasOptimizationName,
		Description: `
		Wraps every AliasIteratorType node in a DispatchIteratorType passthrough
		so the dispatch layer can recognize the (definition, relation) boundary
		as a dispatch candidate without re-deriving it at runtime. Aliases whose
		subtree contains an unmatched RecursiveSentinel are left unwrapped — the
		sentinel must execute inside the collection context established by its
		matching RecursiveIterator, which is broken by an RPC hop.
		`,
		NewTransform: func(_ queryopt.RequestParams) queryopt.OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(outline, []query.OutlineMutation{wrapAliasWithDispatch})
			}
		},
	})
}

// wrapAliasWithDispatch is a bottom-up OutlineMutation that wraps each
// AliasIteratorType node in a DispatchIteratorType node. The wrap is the
// static portion of the old DispatchExecutor.shouldDispatchAlias check:
//   - the "is this an alias" test becomes "is this outline node an alias?"
//   - the sentinel-recursion guard runs over the outline subtree
//
// The remaining runtime guard (in-progress dispatch-key cycle break) cannot
// be made static and stays in the executor.
func wrapAliasWithDispatch(outline query.Outline) query.Outline {
	if outline.Type != query.AliasIteratorType {
		return outline
	}
	if containsUnmatchedRecursiveSentinelOutline(outline) {
		return outline
	}
	// New node has ID==0; ApplyOptimizations runs FillMissingNodeIDs after the
	// transform, which assigns it a fresh ID and records its canonical key.
	return query.Outline{
		Type:        DispatchIteratorType,
		SubOutlines: []query.Outline{outline},
	}
}

// recursiveKey uniquely identifies a recursion pair by definition and relation
// name. Used by containsUnmatchedRecursiveSentinelOutline to pair up sentinels
// with their managing RecursiveIterators at optimization time.
type recursiveKey struct {
	definitionName string
	relationName   string
}

// containsUnmatchedRecursiveSentinelOutline walks an outline subtree and
// reports whether any RecursiveSentinel has no RecursiveIterator with the
// same (definition, relation) key in the same subtree. An unmatched sentinel
// means dispatching this subtree would sever the sentinel from the collection
// context its iterator establishes — the optimizer uses this to keep such
// aliases unwrapped (and therefore non-dispatching) at planning time.
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

// ApplyDispatchWrap runs only the dispatch-wrap-alias optimization on the
// given CanonicalOutline. It exists for callers (dispatch service handlers,
// query plan compilers) that already ran queryopt.OptimizersForRequest and
// want to bolt on the dispatch wrap as a final pass.
func ApplyDispatchWrap(co query.CanonicalOutline, params queryopt.RequestParams) (query.CanonicalOutline, error) {
	opt, err := queryopt.GetOptimization(DispatchWrapAliasOptimizationName)
	if err != nil {
		return query.CanonicalOutline{}, err
	}
	return queryopt.ApplyOptimizations(co, []queryopt.Optimizer{opt}, params)
}
