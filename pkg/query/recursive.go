package query

import (
	"fmt"
)

const defaultMaxRecursionDepth = 50

var _ Iterator = &RecursiveIterator{}

// RecursiveIterator is the root controller that manages iterative deepening for recursive schemas.
// It wraps an iterator tree that contains RecursiveSentinel sentinels, and executes the tree
// repeatedly with increasing depth until a fixed point is reached or max depth is exceeded.
type RecursiveIterator struct {
	templateTree Iterator
	sentinels    []*RecursiveSentinel
}

// NewRecursiveIterator creates a new recursive iterator controller
func NewRecursiveIterator(templateTree Iterator, sentinels []*RecursiveSentinel) *RecursiveIterator {
	return &RecursiveIterator{
		templateTree: templateTree,
		sentinels:    sentinels,
	}
}

// CheckImpl implements iterative deepening for Check operations
func (r *RecursiveIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return r.iterativeDeepening(ctx, func(ctx *Context, tree Iterator) (PathSeq, error) {
		return ctx.Check(tree, resources, subject)
	})
}

// IterSubjectsImpl implements iterative deepening for IterSubjects operations
func (r *RecursiveIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return r.iterativeDeepening(ctx, func(ctx *Context, tree Iterator) (PathSeq, error) {
		return ctx.IterSubjects(tree, resource)
	})
}

// IterResourcesImpl implements iterative deepening for IterResources operations
func (r *RecursiveIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return r.iterativeDeepening(ctx, func(ctx *Context, tree Iterator) (PathSeq, error) {
		return ctx.IterResources(tree, subject)
	})
}

// iterativeDeepening executes the core iterative deepening algorithm
// It yields results directly, always running to maxDepth to find all valid paths
func (r *RecursiveIterator) iterativeDeepening(ctx *Context, execute func(*Context, Iterator) (PathSeq, error)) (PathSeq, error) {
	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	return func(yield func(Path, error) bool) {
		seen := make(map[string]bool)

		for depth := range maxDepth {
			ctx.TraceStep(r, "Depth %d: starting iteration", depth)

			// Build tree for this depth by deepening the template
			deepenedTree, err := r.buildTreeAtDepth(depth)
			if err != nil {
				return
			}

			// Execute the tree
			pathSeq, err := execute(ctx, deepenedTree)
			if err != nil {
				yield(Path{}, fmt.Errorf("execution failed at depth %d: %w", depth, err))
				return
			}

			newPathCount := 0
			totalPathCount := 0

			// Yield each new path we find
			for path, err := range pathSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}

				totalPathCount++

				// Deduplicate paths by key
				key := path.Key()
				if !seen[key] {
					seen[key] = true
					newPathCount++
					if !yield(path, nil) {
						return
					}
				}
			}

			ctx.TraceStep(r, "Depth %d: collected %d paths (%d new)", depth, totalPathCount, newPathCount)
		}

		ctx.TraceStep(r, "Completed at max depth %d", maxDepth)
	}, nil
}

// buildTreeAtDepth creates a tree for the given depth by replacing placeholders
// with deeper copies of the template tree
func (r *RecursiveIterator) buildTreeAtDepth(depth int) (Iterator, error) {
	var err error
	// Clone and unwrap any nested RecursiveIterators at this depth
	clonedTree := r.templateTree.Clone()
	clonedTree, err = unwrapRecursiveIterators(clonedTree, depth)
	if err != nil {
		return nil, err
	}

	if depth == 0 {
		// At depth 0, sentinels remain as-is (return empty)
		return clonedTree, nil
	}

	// For depth > 0, replace sentinels with depth-1 tree
	deeperTree, err := r.buildTreeAtDepth(depth - 1)
	if err != nil {
		return nil, err
	}
	// Unwrap the replacement tree as well, in case it contains nested RecursiveIterators
	deeperTree, err = unwrapRecursiveIterators(deeperTree, depth-1)
	if err != nil {
		return nil, err
	}

	clonedTree, err = replaceSentinelsInTree(clonedTree, deeperTree)
	if err != nil {
		return nil, err
	}

	// Unwrap again after replacement, as new RecursiveIterators may have been introduced
	return unwrapRecursiveIterators(clonedTree, depth)
}

// unwrapRecursiveIterators recursively unwraps nested RecursiveIterators,
// replacing them with their template trees at the specified depth
func unwrapRecursiveIterators(tree Iterator, depth int) (Iterator, error) {
	return Walk(tree, func(it Iterator) (Iterator, error) {
		if recIt, isRecursive := it.(*RecursiveIterator); isRecursive {
			// Unwrap the RecursiveIterator by building its tree at this depth
			// Note: We need to unwrap recursively in case buildTreeAtDepth returns another RecursiveIterator
			rec, err := recIt.buildTreeAtDepth(depth)
			if err != nil {
				return nil, err
			}
			return unwrapRecursiveIterators(rec, depth)
		}
		return it, nil
	})
}

// replaceSentinelsWithTree walks the iterator tree and replaces all RecursiveSentinel instances
// with a clone of the provided replacement tree
func replaceSentinelsInTree(tree Iterator, replacement Iterator) (Iterator, error) {
	return Walk(tree, func(it Iterator) (Iterator, error) {
		if _, isSentinel := it.(*RecursiveSentinel); isSentinel {
			return replacement.Clone(), nil
		}
		return it, nil
	})
}

// Clone creates a deep copy of the RecursiveIterator
func (r *RecursiveIterator) Clone() Iterator {
	// Clone template tree and sentinels
	clonedSentinels := make([]*RecursiveSentinel, len(r.sentinels))
	for i, s := range r.sentinels {
		clonedSentinels[i] = s.Clone().(*RecursiveSentinel)
	}

	return &RecursiveIterator{
		templateTree: r.templateTree.Clone(),
		sentinels:    clonedSentinels,
	}
}

// Explain returns a description of this recursive iterator
func (r *RecursiveIterator) Explain() Explain {
	sentinelInfo := make([]string, len(r.sentinels))
	for i, s := range r.sentinels {
		sentinelInfo[i] = s.ID()
	}

	return Explain{
		Name: "RecursiveIterator",
		Info: fmt.Sprintf("RecursiveIterator(sentinels: %v)", sentinelInfo),
		SubExplain: []Explain{
			r.templateTree.Explain(),
		},
	}
}

func (r *RecursiveIterator) Subiterators() []Iterator {
	return []Iterator{r.templateTree}
}

func (r *RecursiveIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &RecursiveIterator{
		templateTree: newSubs[0],
		sentinels:    r.sentinels,
	}, nil
}
