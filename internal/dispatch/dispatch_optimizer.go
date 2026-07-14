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

// wrapAliasWithDispatch is a bottom-up OutlineMutation that wraps an
// AliasIteratorType node in a DispatchIteratorType when dispatching the alias
// is worth the per-dispatch overhead. Three exclusions:
//   - non-aliases are passed through untouched;
//   - aliases whose subtree contains an unmatched RecursiveSentinel can't be
//     dispatched without severing the sentinel's collection context;
//   - aliases whose body is "leaf-like" (a single Datastore read, or a Union
//     whose direct children are all Datastores) are too cheap to amortize the
//     dispatch wrap — see aliasBodyIsLeafLike for the rationale.
//
// After Phase 1's executor migration, the wrap is the authoritative dispatch
// boundary: every wrap → a dispatch, no wrap → local execution. So skipping
// a wrap here means skipping the dispatch entirely.
func wrapAliasWithDispatch(outline query.Outline) query.Outline {
	if outline.Type != query.AliasIteratorType {
		return outline
	}
	if containsUnmatchedRecursiveSentinelOutline(outline) {
		return outline
	}
	if aliasBodyIsLeafLike(outline) {
		return outline
	}
	// New node has ID==0; ApplyOptimizations runs FillMissingNodeIDs after the
	// transform, which assigns it a fresh ID and records its canonical key.
	return query.Outline{
		Type:        DispatchIteratorType,
		SubOutlines: []query.Outline{outline},
	}
}

// aliasBodyIsLeafLike reports whether the alias's single body is too cheap to
// be worth wrapping for dispatch:
//   - a bare DatastoreIterator (one relationship read), or
//   - a Union whose direct children are all DatastoreIterators (a fan-in of
//     reads to compose multiple direct relations like `view = viewer + editor`).
//
// "Direct children" only — we intentionally don't recurse into nested Unions.
// Anything richer than this (an Arrow, an Intersection, a Recursive, a nested
// Alias-now-wrapped-as-Dispatch) is heavy enough that dispatch + caching pays
// for itself, so the wrap stays.
//
// Optimizers run bottom-up via MutateOutline, so the body we inspect here is
// the post-mutation outline. If a descendant alias was itself a candidate for
// wrapping it has already become DispatchIteratorType — which is not Datastore,
// so this check correctly classifies the parent as "not leaf-like" and keeps
// its wrap.
func aliasBodyIsLeafLike(outline query.Outline) bool {
	if len(outline.SubOutlines) != 1 {
		return false
	}
	body := outline.SubOutlines[0]
	switch body.Type {
	case query.DatastoreIteratorType:
		return true
	case query.UnionIteratorType:
		if len(body.SubOutlines) == 0 {
			return false
		}
		for _, child := range body.SubOutlines {
			if child.Type != query.DatastoreIteratorType {
				return false
			}
		}
		return true
	}
	return false
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
