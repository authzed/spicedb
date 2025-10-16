package query

import (
	"fmt"
)

const defaultMaxRecursionDepth = 10

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
func (r *RecursiveIterator) iterativeDeepening(ctx *Context, execute func(*Context, Iterator) (PathSeq, error)) (PathSeq, error) {
	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	var finalResults []Path

	for depth := 0; depth < maxDepth; depth++ {
		ctx.TraceStep(r, "Depth %d: starting iteration", depth)

		// Build tree for this depth by deepening the template
		deepenedTree := r.buildTreeAtDepth(depth)

		// Execute the tree
		pathSeq, err := execute(ctx, deepenedTree)
		if err != nil {
			return nil, fmt.Errorf("execution failed at depth %d: %w", depth, err)
		}

		// Collect all paths from this iteration
		currentResults := make([]Path, 0)
		for path, err := range pathSeq {
			if err != nil {
				return nil, err
			}
			currentResults = append(currentResults, path)
		}

		ctx.TraceStep(r, "Depth %d: collected %d paths", depth, len(currentResults))
		finalResults = currentResults
	}

	// Return results from final depth
	ctx.TraceStep(r, "Completed at max depth %d", maxDepth)
	return pathSeqFromPaths(finalResults), nil
}

// buildTreeAtDepth creates a tree for the given depth by replacing placeholders
// with deeper copies of the template tree
func (r *RecursiveIterator) buildTreeAtDepth(depth int) Iterator {
	if depth == 0 {
		// At depth 0, placeholders remain as-is (return empty)
		return r.templateTree.Clone()
	}

	// For depth > 0, replace placeholders with depth-1 tree
	clonedTree := r.templateTree.Clone()
	deeperTree := r.buildTreeAtDepth(depth - 1)
	// Unwrap nested RecursiveIterators to avoid independent depth tracking
	deeperTree = unwrapRecursiveIterators(deeperTree, depth-1)
	r.replacePlaceholdersWithTree(clonedTree, deeperTree)
	return clonedTree
}

// unwrapRecursiveIterators recursively unwraps nested RecursiveIterators,
// replacing them with their template trees at the specified depth
func unwrapRecursiveIterators(tree Iterator, depth int) Iterator {
	if recIt, isRecursive := tree.(*RecursiveIterator); isRecursive {
		// Unwrap the RecursiveIterator by building its tree at this depth
		return unwrapRecursiveIterators(recIt.buildTreeAtDepth(depth), depth)
	}
	return tree
}

// replacePlaceholdersWithTree walks the iterator tree and replaces all RecursiveSentinel instances
// with a clone of the provided replacement tree
func (r *RecursiveIterator) replacePlaceholdersWithTree(tree Iterator, replacement Iterator) {
	replacePlaceholdersWithTreeRecursive(tree, replacement)
}

// replacePlaceholdersWithTreeRecursive is a visitor function that walks the tree and performs replacement
func replacePlaceholdersWithTreeRecursive(it Iterator, replacement Iterator) {
	switch iter := it.(type) {
	case *Union:
		for i, subIt := range iter.subIts {
			if _, isSentinel := subIt.(*RecursiveSentinel); isSentinel {
				iter.subIts[i] = replacement.Clone()
			} else {
				replacePlaceholdersWithTreeRecursive(subIt, replacement)
			}
		}

	case *Intersection:
		for i, subIt := range iter.subIts {
			if _, isSentinel := subIt.(*RecursiveSentinel); isSentinel {
				iter.subIts[i] = replacement.Clone()
			} else {
				replacePlaceholdersWithTreeRecursive(subIt, replacement)
			}
		}

	case *Arrow:
		if _, isSentinel := iter.left.(*RecursiveSentinel); isSentinel {
			iter.left = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.left, replacement)
		}

		if _, isSentinel := iter.right.(*RecursiveSentinel); isSentinel {
			iter.right = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.right, replacement)
		}

	case *IntersectionArrow:
		if _, isSentinel := iter.left.(*RecursiveSentinel); isSentinel {
			iter.left = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.left, replacement)
		}

		if _, isSentinel := iter.right.(*RecursiveSentinel); isSentinel {
			iter.right = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.right, replacement)
		}

	case *Exclusion:
		if _, isSentinel := iter.mainSet.(*RecursiveSentinel); isSentinel {
			iter.mainSet = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.mainSet, replacement)
		}

		if _, isSentinel := iter.excluded.(*RecursiveSentinel); isSentinel {
			iter.excluded = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.excluded, replacement)
		}

	case *Alias:
		if _, isSentinel := iter.subIt.(*RecursiveSentinel); isSentinel {
			iter.subIt = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.subIt, replacement)
		}

	case *CaveatIterator:
		if _, isSentinel := iter.subiterator.(*RecursiveSentinel); isSentinel {
			iter.subiterator = replacement.Clone()
		} else {
			replacePlaceholdersWithTreeRecursive(iter.subiterator, replacement)
		}

	case *RecursiveSentinel, *FixedIterator, *RelationIterator, *RecursiveIterator:
		// Leaf nodes or nested recursive iterators - no children to process
		return

	default:
		// Unknown iterator type - assume it's a leaf or doesn't contain sentinels
		// This handles custom iterators like test iterators gracefully
		return
	}
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
