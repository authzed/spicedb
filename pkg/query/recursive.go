package query

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultMaxRecursionDepth = 50

var _ Iterator = &RecursiveIterator{}

// RecursiveIterator is the root controller that manages iterative deepening for recursive schemas.
// It wraps an iterator tree that contains RecursiveSentinel sentinels, and executes the tree
// repeatedly with increasing depth until a fixed point is reached or max depth is exceeded.
type RecursiveIterator struct {
	id             string
	templateTree   Iterator
	definitionName string // The schema definition this iterator is recursing on
	relationName   string // The relation name this iterator is recursing on
}

// NewRecursiveIterator creates a new recursive iterator controller
func NewRecursiveIterator(templateTree Iterator, definitionName, relationName string) *RecursiveIterator {
	return &RecursiveIterator{
		id:             uuid.NewString(),
		templateTree:   templateTree,
		definitionName: definitionName,
		relationName:   relationName,
	}
}

// CheckImpl implements iterative deepening for Check operations
func (r *RecursiveIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return r.iterativeDeepening(ctx, func(ctx *Context, tree Iterator) (PathSeq, error) {
		return ctx.Check(tree, resources, subject)
	})
}

// IterSubjectsImpl implements BFS traversal for IterSubjects operations
func (r *RecursiveIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return r.breadthFirstIterSubjects(ctx, resource, filterSubjectType)
}

// IterResourcesImpl implements BFS traversal for IterResources operations
func (r *RecursiveIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return r.breadthFirstIterResources(ctx, subject, filterResourceType)
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

	clonedTree, err = r.replaceSentinelsInTree(clonedTree, deeperTree)
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

// replaceSentinelsInTree walks the iterator tree and replaces RecursiveSentinel instances
// that match this RecursiveIterator's definition and relation with a clone of the provided replacement tree.
// Non-matching sentinels are left alone as they belong to different RecursiveIterators.
func (r *RecursiveIterator) replaceSentinelsInTree(tree Iterator, replacement Iterator) (Iterator, error) {
	return Walk(tree, func(it Iterator) (Iterator, error) {
		if sentinel, isSentinel := it.(*RecursiveSentinel); isSentinel {
			// Only replace sentinels that belong to THIS RecursiveIterator's schema
			if sentinel.DefinitionName() == r.definitionName && sentinel.RelationName() == r.relationName {
				return replacement.Clone(), nil
			}
			// Leave non-matching sentinels alone (they belong to a different RecursiveIterator)
			return sentinel, nil
		}
		return it, nil
	})
}

// Clone creates a deep copy of the RecursiveIterator
func (r *RecursiveIterator) Clone() Iterator {
	return &RecursiveIterator{
		id:             uuid.NewString(),
		templateTree:   r.templateTree.Clone(),
		definitionName: r.definitionName,
		relationName:   r.relationName,
	}
}

// Explain returns a description of this recursive iterator
func (r *RecursiveIterator) Explain() Explain {
	return Explain{
		Name: "RecursiveIterator",
		Info: "RecursiveIterator",
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
		id:             uuid.NewString(),
		templateTree:   newSubs[0],
		definitionName: r.definitionName,
		relationName:   r.relationName,
	}, nil
}

func (r *RecursiveIterator) ID() string {
	return r.id
}

func (r *RecursiveIterator) ResourceType() (ObjectType, error) {
	// Delegate to the template tree
	return r.templateTree.ResourceType()
}

func (r *RecursiveIterator) SubjectTypes() ([]ObjectType, error) {
	// Delegate to the template tree
	return r.templateTree.SubjectTypes()
}

// breadthFirstIterSubjects implements BFS traversal for IterSubjects operations.
func (r *RecursiveIterator) breadthFirstIterSubjects(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	ctx.TraceStep(r, "BFS IterSubjects starting with resource %s:%s", resource.ObjectType, resource.ObjectID)

	return breadthFirstIter(
		ctx,
		r,
		resource,
		// Key function: get unique key for a node
		func(node Object) string {
			return node.Key()
		},
		// Execute: iterate subjects for a frontier object
		func(depth1Tree Iterator, frontierNode Object) (PathSeq, error) {
			return ctx.IterSubjects(depth1Tree, frontierNode, filterSubjectType)
		},
		// Extract recursive node from path
		func(path Path) (Object, bool) {
			if r.isRecursiveSubject(path.Subject) {
				return GetObject(path.Subject), true
			}
			return Object{}, false
		},
	)
}

// breadthFirstIterResources implements BFS traversal for IterResources operations.
func (r *RecursiveIterator) breadthFirstIterResources(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	ctx.TraceStep(r, "BFS IterResources starting with subject %s:%s#%s",
		subject.ObjectType, subject.ObjectID, subject.Relation)

	return breadthFirstIter(
		ctx,
		r,
		subject,
		ObjectAndRelationKey, // No need for a closure, just call directly!
		// Execute: iterate resources for a frontier subject
		func(depth1Tree Iterator, frontierNode ObjectAndRelation) (PathSeq, error) {
			return ctx.IterResources(depth1Tree, frontierNode, filterResourceType)
		},
		// Extract recursive node from path
		func(path Path) (ObjectAndRelation, bool) {
			if r.isRecursiveResource(path.Resource) {
				return path.Resource.WithEllipses(), true
			}
			return ObjectAndRelation{}, false
		},
	)
}

// breadthFirstIter implements the core BFS algorithm for recursive iteration.
// It is a generic function that works with both Object and ObjectAndRelation types.
func breadthFirstIter[T any](
	ctx *Context,
	r *RecursiveIterator,
	startNode T,
	keyFn func(node T) string,
	executeFn func(depth1Tree Iterator, frontierNode T) (PathSeq, error),
	extractNodeFn func(Path) (node T, isRecursive bool),
) (PathSeq, error) {
	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	// Build depth-1 tree once (one level of recursive expansion)
	depth1Tree, err := r.buildTreeAtDepth(1)
	if err != nil {
		return nil, err
	}

	return func(yield func(Path, error) bool) {
		// Track seen paths globally by endpoints (for cross-ply deduplication)
		pathsByEndpoint := make(map[string]Path)

		// Track seen recursive nodes to prevent cycles
		seenRecursiveNodes := make(map[string]bool)
		seenRecursiveNodes[keyFn(startNode)] = true

		// Initialize frontier with starting node
		currentFrontier := []T{startNode}

		for ply := 0; ply < maxDepth && len(currentFrontier) > 0; ply++ {
			ctx.TraceStep(r, "Ply %d: exploring %d frontier nodes", ply, len(currentFrontier))

			// Collect paths from this ply by endpoint
			plyPaths := make(map[string]Path)
			var nextFrontier []T

			for _, frontierNode := range currentFrontier {
				// Execute depth-1 tree on this node
				pathSeq, err := executeFn(depth1Tree, frontierNode)
				if err != nil {
					yield(Path{}, fmt.Errorf("execution failed at ply %d: %w", ply, err))
					return
				}

				for path, err := range pathSeq {
					if err != nil {
						yield(Path{}, err)
						return
					}

					// Merge paths by endpoint with OR semantics
					endpointKey := path.EndpointsKey()
					if existing, found := plyPaths[endpointKey]; found {
						merged, err := existing.MergeOr(path)
						if err != nil {
							yield(Path{}, fmt.Errorf("failed to merge paths: %w", err))
							return
						}
						plyPaths[endpointKey] = merged
					} else {
						plyPaths[endpointKey] = path
					}

					// Extract recursive nodes for next ply
					if node, isRecursive := extractNodeFn(path); isRecursive {
						nodeKey := keyFn(node)
						if !seenRecursiveNodes[nodeKey] {
							seenRecursiveNodes[nodeKey] = true
							nextFrontier = append(nextFrontier, node)
							ctx.TraceStep(r, "Found recursive node: %s", nodeKey)
						}
					}
				}
			}

			// Yield new paths and update global map
			newPathCount := 0
			for endpointKey, path := range plyPaths {
				if existing, found := pathsByEndpoint[endpointKey]; found {
					// Endpoint already seen in previous ply - merge but don't re-yield
					merged, err := existing.MergeOr(path)
					if err != nil {
						yield(Path{}, fmt.Errorf("failed to merge paths globally: %w", err))
						return
					}
					pathsByEndpoint[endpointKey] = merged
				} else {
					// New endpoint - add to global map and yield
					pathsByEndpoint[endpointKey] = path
					newPathCount++
					if !yield(path, nil) {
						return
					}
				}
			}

			ctx.TraceStep(r, "Ply %d: found %d unique paths (%d new), %d nodes for next ply",
				ply, len(plyPaths), newPathCount, len(nextFrontier))

			currentFrontier = nextFrontier
		}

		if len(currentFrontier) == 0 {
			ctx.TraceStep(r, "BFS completed (no more recursive nodes)")
		} else {
			ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
		}
	}, nil
}

// isRecursiveSubject checks if a subject represents a recursive node that should be explored further.
func (r *RecursiveIterator) isRecursiveSubject(subject ObjectAndRelation) bool {
	// Must match the definition type
	if subject.ObjectType != r.definitionName {
		return false
	}

	// Must match the relation or be ellipsis/empty
	// Empty relation means the subject reference doesn't specify a relation
	// Ellipsis means "any relation on this object"
	if subject.Relation != r.relationName &&
		subject.Relation != "" &&
		subject.Relation != tuple.Ellipsis {
		return false
	}

	return true
}

// isRecursiveResource checks if a resource represents a recursive node that should be explored further.
func (r *RecursiveIterator) isRecursiveResource(resource Object) bool {
	// Resources don't have relations, just check type
	return resource.ObjectType == r.definitionName
}
