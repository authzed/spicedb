package query

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	MustRegisterIterator(IteratorSpec{
		Type: RecursiveIteratorType,
		Name: "Recursive",
		ConstructWithArgs: func(args *IteratorArgs, subs []Iterator, key CanonicalKey) (Iterator, error) {
			if len(subs) != 1 {
				return nil, fmt.Errorf("RecursiveIterator requires exactly 1 subiterator, got %d", len(subs))
			}
			if args == nil || args.DefinitionName == "" || args.RelationName == "" {
				return nil, errors.New("RecursiveIterator requires DefinitionName and RelationName in Args")
			}
			recursive := NewRecursiveIterator(subs[0], args.DefinitionName, args.RelationName)
			recursive.canonicalKey = key
			return recursive, nil
		},
		Deserialize: deserializeRecursive,
	})
}

// frontierEntry is a lightweight frontier node for BFS IterSubjects.
// It carries only the fields needed to combine with the next hop's path —
// unlike a full *Path it does not hold Resource, Relation, or Metadata.
// Condition is the canonical caveat condition under which Subject was reached,
// which is conjoined with each outgoing edge's caveat as the frontier advances.
type frontierEntry struct {
	Subject    ObjectAndRelation
	Condition  caveats.Condition
	Expiration *time.Time
	Integrity  []*core.RelationshipIntegrity
}

const defaultMaxRecursionDepth = 50

// recursiveCheckStrategy specifies which strategy to use for Check operations
type recursiveCheckStrategy int

const (
	// recursiveCheckIterSubjects calls IterSubjects for each resource, filters by subject
	recursiveCheckIterSubjects recursiveCheckStrategy = iota
	// recursiveCheckIterResources calls IterResources with subject, filters by resources
	recursiveCheckIterResources
	// recursiveCheckDeepening uses iterative deepening
	recursiveCheckDeepening
)

var _ Iterator = &RecursiveIterator{}

// RecursiveIterator is the root controller that manages iterative deepening for recursive schemas.
// It wraps an iterator tree that contains RecursiveSentinel sentinels, and executes the tree
// repeatedly with increasing depth until a fixed point is reached or max depth is exceeded.
type RecursiveIterator struct {
	templateTree   Iterator
	definitionName string                 // The schema definition this iterator is recursing on
	relationName   string                 // The relation name this iterator is recursing on
	checkStrategy  recursiveCheckStrategy // strategy for Check operations
	canonicalKey   CanonicalKey
}

// NewRecursiveIterator creates a new recursive iterator controller
func NewRecursiveIterator(templateTree Iterator, definitionName, relationName string) *RecursiveIterator {
	return &RecursiveIterator{
		templateTree:   templateTree,
		definitionName: definitionName,
		relationName:   relationName,
		checkStrategy:  recursiveCheckIterSubjects, // default strategy
	}
}

// DefinitionName returns the definition name this iterator is recursing on
func (r *RecursiveIterator) DefinitionName() string {
	return r.definitionName
}

// RelationName returns the relation name this iterator is recursing on
func (r *RecursiveIterator) RelationName() string {
	return r.relationName
}

// findMatchingSentinels walks the template tree and returns canonical key hashes of sentinels that match
// this RecursiveIterator's definition and relation (but stops at nested RecursiveIterators).
func (r *RecursiveIterator) findMatchingSentinels() []uint64 {
	var sentinelHashes []uint64
	_, _ = Walk(r.templateTree, func(it Iterator) (Iterator, error) {
		// Stop traversing if we encounter a nested RecursiveIterator
		if _, isRecursive := it.(*RecursiveIterator); isRecursive {
			return it, nil // Don't traverse into nested RecursiveIterators
		}

		// Collect matching sentinels
		if sentinel, ok := it.(*RecursiveSentinelIterator); ok {
			if sentinel.DefinitionName() == r.definitionName &&
				sentinel.RelationName() == r.relationName {
				sentinelHashes = append(sentinelHashes, sentinel.CanonicalKey().Hash())
			}
		}
		return it, nil
	})
	return sentinelHashes
}

// CheckImpl implements traversal for Check operations with strategy selection
func (r *RecursiveIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	switch r.checkStrategy {
	case recursiveCheckIterSubjects:
		return r.recursiveCheckIterSubjects(ctx, resource, subject)
	case recursiveCheckIterResources:
		return r.recursiveCheckIterResources(ctx, resource, subject)
	case recursiveCheckDeepening:
		return r.deepeningCheck(ctx, resource, subject)
	default:
		return nil, fmt.Errorf("unknown recursive check strategy: %d", r.checkStrategy)
	}
}

// IterSubjectsImpl implements BFS traversal for IterSubjects operations
func (r *RecursiveIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return r.breadthFirstIterSubjects(ctx, resource, filterSubjectType)
}

// IterResourcesImpl implements BFS traversal for IterResources operations
func (r *RecursiveIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return r.breadthFirstIterResources(ctx, subject, filterResourceType)
}

// buildTreeAtDepth creates a tree for the given depth by replacing placeholders
// with deeper copies of the template tree. Used by breadthFirstIter for IterSubjects.
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
// replacing them with their template trees at the specified depth.
// Used by buildTreeAtDepth for IterSubjects.
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
		if sentinel, isSentinel := it.(*RecursiveSentinelIterator); isSentinel {
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
		canonicalKey:   r.canonicalKey,
		templateTree:   r.templateTree.Clone(),
		definitionName: r.definitionName,
		relationName:   r.relationName,
		checkStrategy:  r.checkStrategy, // preserve strategy
	}
}

// Explain returns a description of this recursive iterator
func (r *RecursiveIterator) Explain() Explain {
	return Explain{
		Name: "Recursive",
		Info: "Recursive",
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
		canonicalKey:   r.canonicalKey,
		templateTree:   newSubs[0],
		definitionName: r.definitionName,
		relationName:   r.relationName,
		checkStrategy:  r.checkStrategy, // preserve strategy
	}, nil
}

func (r *RecursiveIterator) CanonicalKey() CanonicalKey {
	return r.canonicalKey
}

func (r *RecursiveIterator) ResourceType() ([]ObjectType, error) {
	// Delegate to the template tree
	return r.templateTree.ResourceType()
}

func (r *RecursiveIterator) SubjectTypes() ([]ObjectType, error) {
	// Delegate to the template tree
	return r.templateTree.SubjectTypes()
}

// breadthFirstIterSubjects implements BFS traversal for IterSubjects operations.
// Uses context-based frontier collection: the sentinel collects queried resources during execution,
// which are then used to build the frontier for the next ply.
func (r *RecursiveIterator) breadthFirstIterSubjects(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "BFS IterSubjects: resource=%s:%s, filter=%s",
			resource.ObjectType, resource.ObjectID, filterSubjectType.Type)
	}

	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	// Find all matching sentinels in the template tree
	sentinelIDs := r.findMatchingSentinels()
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "Found %d matching sentinels: %v", len(sentinelIDs), sentinelIDs)
	}

	return func(yield func(*Path, error) bool) {
		// yieldedPaths accumulates every endpoint path, OR-merged by endpoint key.
		// Results are buffered and flushed only once the traversal converges: under a
		// caveated schema a later ply can weaken the condition on an already-seen
		// endpoint (an object first reached via a caveated edge and later reached
		// unconditionally), so emitting eagerly could leak a stale, over-restrictive
		// caveat (bug B1). Buffering adds no memory — every path was retained here
		// already for cross-ply deduplication.
		yieldedPaths := make(map[string]*Path)

		// reached records, for each object that has entered the frontier, the
		// canonical caveat condition under which it was reached. An object is
		// re-expanded only when a new path *weakens* that condition — the semi-naive
		// fixpoint. Because the condition is canonical, conjunction is idempotent
		// (c1 ∧ c2 ∧ c1 == {c1,c2}) and the unconditional case is absorbing under OR,
		// so the fixpoint terminates even on cyclic data.
		reached := make(map[string]caveats.Condition)

		reached[resource.Key()] = caveats.Top()
		frontier := []frontierEntry{
			{
				Subject: ObjectAndRelation{
					ObjectType: resource.ObjectType,
					ObjectID:   resource.ObjectID,
					Relation:   tuple.Ellipsis,
				},
				Condition: caveats.Top(),
			},
		}

		// plyPaths is allocated once and cleared each ply to avoid per-ply allocations.
		plyPaths := make(map[string]*Path)

		// nextFrontier is reused across plies.
		var nextFrontier []frontierEntry

		for ply := 0; ply < maxDepth; ply++ {
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: %d frontier entries", ply, len(frontier))
			}

			// Enable collection mode for all matching sentinels.
			for _, sentinelID := range sentinelIDs {
				ctx.EnableFrontierCollection(sentinelID)
			}

			// Clear the ply map and reuse its backing storage.
			clear(plyPaths)

			// Query IterSubjects FROM each frontier object, accumulating results in plyPaths.
			// Each edge's caveat is conjoined with the condition under which the frontier
			// object itself was reached; endpoints reached multiple ways this ply are ORed.
			for _, fe := range frontier {
				frontierResource := GetObject(fe.Subject)

				if ctx.shouldTrace() {
					ctx.TraceStep(r, "Ply %d: querying from %s:%s",
						ply, frontierResource.ObjectType, frontierResource.ObjectID)
				}

				subSeq, err := ctx.IterSubjects(r.templateTree, frontierResource, NoObjectFilter())
				if err != nil {
					yield(nil, fmt.Errorf("execution failed at ply %d: %w", ply, err))
					return
				}

				for subPath, err := range subSeq {
					if err != nil {
						yield(nil, fmt.Errorf("execution failed at ply %d: %w", ply, err))
						return
					}

					// Combine frontier entry with sub-path to get full path from original resource:
					//   fe:      original_resource → frontier_resource  (implicit)
					//   subPath: frontier_resource → subject
					//   result:  original_resource → subject
					pathCondition := fe.Condition.And(caveats.FromExpression(subPath.Caveat))
					combinedPath := &Path{
						Resource:   resource,
						Relation:   r.relationName,
						Subject:    subPath.Subject,
						Caveat:     pathCondition.Expression(),
						Expiration: combineExpiration(fe.Expiration, subPath.Expiration),
						Integrity:  combineIntegrity(fe.Integrity, subPath.Integrity),
					}

					key := combinedPath.EndpointsKey()
					if existing, seen := plyPaths[key]; seen {
						if _, err := existing.MergeOr(combinedPath); err != nil {
							yield(nil, err)
							return
						}
					} else {
						plyPaths[key] = combinedPath
					}
				}
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: found %d unique paths", ply, len(plyPaths))
			}

			// Extract frontier objects collected by all sentinels during this ply
			// (arrow recursion surfaces its next hop this way rather than as subjects).
			var collectedObjects []Object
			for _, sentinelID := range sentinelIDs {
				collectedObjects = append(collectedObjects, ctx.ExtractFrontierCollection(sentinelID)...)
			}
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: collected %d recursive objects", ply, len(collectedObjects))
			}

			// Reset and reuse nextFrontier.
			nextFrontier = nextFrontier[:0]

			// Merge this ply's endpoints into the global buffer and re-enqueue any
			// recursive object whose reaching condition weakened.
			for key, path := range plyPaths {
				if existing, seen := yieldedPaths[key]; seen {
					if _, err := existing.MergeOr(path); err != nil {
						yield(nil, err)
						return
					}
				} else {
					yieldedPaths[key] = path
				}

				if path.Subject.ObjectType != r.definitionName {
					continue
				}

				objKey := GetObject(path.Subject).Key()
				newCondition, changed := reached[objKey].Or(caveats.FromExpression(path.Caveat))
				if !changed {
					if ctx.shouldTrace() {
						ctx.TraceStep(r, "Ply %d: %s already reached under an equal-or-weaker condition", ply, objKey)
					}
					continue
				}
				reached[objKey] = newCondition
				nextFrontier = append(nextFrontier, frontierEntry{
					Subject:    path.Subject,
					Condition:  newCondition,
					Expiration: path.Expiration,
					Integrity:  path.Integrity,
				})
				if ctx.shouldTrace() {
					ctx.TraceStep(r, "Ply %d: (re)adding %s to next frontier", ply, objKey)
				}
			}

			// Add sentinel-collected objects to the frontier; they are unconditional.
			for _, obj := range collectedObjects {
				objKey := obj.Key()
				newCondition, changed := reached[objKey].Or(caveats.Top())
				if !changed {
					if ctx.shouldTrace() {
						ctx.TraceStep(r, "Ply %d: skipping collected object %s (already unconditional)", ply, objKey)
					}
					continue
				}
				reached[objKey] = newCondition
				nextFrontier = append(nextFrontier, frontierEntry{
					Subject: ObjectAndRelation{
						ObjectType: obj.ObjectType,
						ObjectID:   obj.ObjectID,
						Relation:   tuple.Ellipsis,
					},
					Condition: caveats.Top(),
				})
				if ctx.shouldTrace() {
					ctx.TraceStep(r, "Ply %d: adding collected object %s to frontier", ply, objKey)
				}
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: %d entries for next frontier", ply, len(nextFrontier))
			}

			if len(nextFrontier) == 0 {
				if ctx.shouldTrace() {
					ctx.TraceStep(r, "BFS converged at ply %d; flushing %d paths", ply, len(yieldedPaths))
				}
				// The traversal has converged: flush every buffered endpoint that
				// matches the subject-type filter.
				for _, path := range yieldedPaths {
					if filterSubjectType.Type != "" && path.Subject.ObjectType != filterSubjectType.Type {
						continue
					}
					if !yield(path, nil) {
						return
					}
				}
				return
			}

			// Swap frontier slices — nextFrontier becomes the active frontier.
			// The old frontier slice is reused as the next nextFrontier buffer.
			frontier, nextFrontier = nextFrontier, frontier[:0]
		}

		// Reaching here means the frontier was still non-empty after maxDepth plies;
		// a converged traversal returns early above once nextFrontier is empty. The
		// answer is therefore not fully determined, so surface an error rather than
		// yielding a silently-truncated result set (matching the legacy engine's
		// MaxDepthExceeded behavior).
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
		}
		yield(nil, MaxRecursionDepthError{Depth: maxDepth})
	}, nil
}

// replaceRecursiveSentinel clones the iterator tree and replaces RecursiveSentinel
// nodes matching this RecursiveIterator's definition with the provided replacement iterator.
func (r *RecursiveIterator) replaceRecursiveSentinel(tree Iterator, replacement Iterator) (Iterator, error) {
	// Use existing Walk function to traverse and clone the tree
	return Walk(tree, func(it Iterator) (Iterator, error) {
		// Only replace sentinels that match this RecursiveIterator's definition
		if sentinel, ok := it.(*RecursiveSentinelIterator); ok {
			if sentinel.DefinitionName() == r.definitionName &&
				sentinel.RelationName() == r.relationName {
				return replacement, nil // Replace with Fixed iterator
			}
		}
		return it, nil // Keep node as-is
	})
}

// breadthFirstIterResources implements BFS traversal for IterResources operations.
// It queries with a constant subject at each ply, replacing the RecursiveSentinel with
// a Fixed iterator containing frontier paths from the previous ply.
func (r *RecursiveIterator) breadthFirstIterResources(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "BFS IterResources with constant subject %s:%s#%s",
			subject.ObjectType, subject.ObjectID, subject.Relation)
	}

	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	return func(yield func(*Path, error) bool) {
		// yieldedPaths buffers every resource path, OR-merged by endpoint key, and is
		// flushed only once the traversal converges. As with IterSubjects, a resource
		// first reached via a caveated path can later be reached unconditionally, so
		// emitting eagerly would leak a stale, over-restrictive caveat (bug B1).
		yieldedPaths := make(map[string]*Path)

		// reached records the canonical caveat condition under which each resource has
		// been reached. A resource re-seeds the frontier only when its condition
		// weakens — the semi-naive fixpoint that keeps caveats sound and terminates.
		reached := make(map[string]caveats.Condition)

		// frontier holds the paths whose condition weakened last ply; reused across plies.
		var frontier []Path

		// Start with the original tree (sentinel returns empty at ply 0).
		currentTree := r.templateTree

		for ply := 0; ply < maxDepth; ply++ {
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: querying tree", ply)
			}

			// Query IterResources with the ORIGINAL subject.
			plySeq, err := ctx.IterResources(currentTree, subject, filterResourceType)
			if err != nil {
				yield(nil, err)
				return
			}

			// Reset the frontier accumulator, reusing backing array.
			frontier = frontier[:0]

			for path, err := range plySeq {
				if err != nil {
					yield(nil, err)
					return
				}

				key := path.EndpointsKey()
				if existing, seen := yieldedPaths[key]; seen {
					if _, err := existing.MergeOr(path); err != nil {
						yield(nil, err)
						return
					}
				} else {
					pathCopy := *path
					yieldedPaths[key] = &pathCopy
				}

				// Re-seed the frontier only if this resource's reaching condition
				// weakened; the frontier path carries the canonical accumulated
				// condition so the next ply combines edges against the weakest form.
				newCondition, changed := reached[key].Or(caveats.FromExpression(path.Caveat))
				if !changed {
					continue
				}
				reached[key] = newCondition
				frontierPath := *path
				frontierPath.Caveat = newCondition.Expression()
				frontier = append(frontier, frontierPath)
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: %d frontier paths", ply, len(frontier))
			}

			// No condition weakened this ply: the fixpoint has converged. Flush the
			// buffered results.
			if len(frontier) == 0 {
				if ctx.shouldTrace() {
					ctx.TraceStep(r, "BFS converged at ply %d; flushing %d paths", ply, len(yieldedPaths))
				}
				for _, path := range yieldedPaths {
					if !yield(path, nil) {
						return
					}
				}
				return
			}

			// Replace sentinel with the weakened frontier for the next ply.
			fixedFrontier := NewFixedIterator(frontier...)
			modifiedTree, err := r.replaceRecursiveSentinel(r.templateTree, fixedFrontier)
			if err != nil {
				yield(nil, fmt.Errorf("failed to replace sentinel: %w", err))
				return
			}
			currentTree = modifiedTree
		}

		// Reaching here means a condition was still weakening after maxDepth plies;
		// a converged traversal returns early above. Surface an error rather than
		// yielding a silently-truncated result set.
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
		}
		yield(nil, MaxRecursionDepthError{Depth: maxDepth})
	}, nil
}

// deepeningCheck implements a deepening traversal for Check operations.
// Unlike IterResources which builds a frontier of paths, deepeningCheck uses iterative deepening
// with early termination: at each ply, we allow one more level of recursion through the
// sentinel by replacing it with progressively deeper trees.
func (r *RecursiveIterator) deepeningCheck(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	// Try increasing ply depths until we find a match or reach max depth.
	// OR-merge paths found at the same resource across plies (different recursive routes
	// through the graph may yield paths with different caveats).
	var result *Path
	foundAtPreviousPly := false

	for ply := 0; ply < maxDepth; ply++ {
		if ctx.shouldTrace() {
			ctx.TraceStep(r, "BFS Check: Ply %d starting", ply)
		}

		// Build tree for this ply by replacing sentinel with ply-depth tree
		// At ply 0: sentinel returns empty (no recursion)
		// At ply 1: sentinel replaced with depth-0 tree (1 level of recursion)
		// At ply 2: sentinel replaced with depth-1 tree (2 levels of recursion)
		plyTree, err := r.buildTreeAtDepth(ply)
		if err != nil {
			return nil, fmt.Errorf("failed to build tree at ply %d: %w", ply, err)
		}

		// Execute Check with the ply tree
		plyPath, err := ctx.Check(plyTree, resource, subject)
		if err != nil {
			return nil, fmt.Errorf("check failed at ply %d: %w", ply, err)
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(r, "BFS Check: Ply %d found=%v", ply, plyPath != nil)
		}

		if plyPath != nil {
			result, err = result.MergeOr(plyPath)
			if err != nil {
				return nil, err
			}
		}

		// Early termination: if we previously found a path but this ply adds nothing new,
		// we've reached a fixed point.
		if plyPath == nil && foundAtPreviousPly {
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "BFS Check: Terminated at ply %d (fixed point reached)", ply)
			}
			break
		}

		if plyPath != nil {
			foundAtPreviousPly = true
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "BFS Check: completed, found=%v", result != nil)
	}
	return result, nil
}

// recursiveCheckIterSubjects implements Check by calling IterSubjects for the resource
// and filtering paths to match the input subject.
func (r *RecursiveIterator) recursiveCheckIterSubjects(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(r, "Check via IterSubjects: processing resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	// Reflexive identity fast path: if the target subject is the resource itself,
	// the templateTree's Check (alias self-edge synthesis) resolves it without the
	// datastore probe that IterSubjects-based BFS would trigger. This matches the
	// dispatcher's MEMBER-when-resource-equals-subject behavior for relations that
	// allow themselves as subjects (e.g. `relation member: user | group#member`)
	// without paying the cost of full BFS enumeration just to look up identity.
	if resource.ObjectType == subject.ObjectType && resource.ObjectID == subject.ObjectID {
		path, err := ctx.Check(r.templateTree, resource, subject)
		if err != nil {
			return nil, err
		}
		if path != nil {
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Check via IterSubjects: matched reflexive identity, skipping BFS")
			}
			return path, nil
		}
	}

	// Get subject type for filtering (type only, not relation - ellipsis is not a real relation)
	filterSubjectType := ObjectType{Type: subject.ObjectType}

	// Call IterSubjects on the RecursiveIterator itself - this will use BFS
	pathSeq, err := ctx.IterSubjects(r, resource, filterSubjectType)
	if err != nil {
		return nil, fmt.Errorf("IterSubjects failed for resource %s:%s: %w", resource.ObjectType, resource.ObjectID, err)
	}

	// Return the first path whose subject matches the input subject (type and ID only).
	// OR-merge if multiple BFS routes produce paths to the same subject with different caveats.
	var result *Path
	for path, err := range pathSeq {
		if err != nil {
			return nil, err
		}
		if GetObject(path.Subject).Equals(GetObject(subject)) {
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Check via IterSubjects: found matching path")
			}
			result, err = result.MergeOr(path)
			if err != nil {
				return nil, err
			}
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "Check via IterSubjects: completed, found=%v", result != nil)
	}
	return result, nil
}

// recursiveCheckIterResources implements Check by calling IterResources with the subject
// and filtering paths to match the input resource.
func (r *RecursiveIterator) recursiveCheckIterResources(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	filterResourceType := ObjectType{Type: resource.ObjectType}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "Check via IterResources: processing subject %s:%s#%s",
			subject.ObjectType, subject.ObjectID, subject.Relation)
	}

	// Call IterResources on the RecursiveIterator itself - this will use BFS
	pathSeq, err := ctx.IterResources(r, subject, filterResourceType)
	if err != nil {
		return nil, fmt.Errorf("IterResources failed for subject %s: %w", subject.String(), err)
	}

	// Return the first path whose resource matches the input resource.
	// OR-merge if multiple routes produce paths with different caveats.
	var result *Path
	for path, err := range pathSeq {
		if err != nil {
			return nil, err
		}
		if path.Resource.Equals(resource) {
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Check via IterResources: found matching path from %s to %s",
					path.Resource.Key(), path.Subject.String())
			}
			result, err = result.MergeOr(path)
			if err != nil {
				return nil, err
			}
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(r, "Check via IterResources: completed, found=%v", result != nil)
	}
	return result, nil
}

const recursiveFlagStrategy = 0 // strategy byte follows if set (default == iter-subjects)

func (r *RecursiveIterator) Serialize(w io.Writer) error {
	return SerializeWithHeader(w, RecursiveIteratorType, r.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		nonDefault := r.checkStrategy != recursiveCheckIterSubjects
		setFlag(&flags, recursiveFlagStrategy, nonDefault)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if err := writeString(buf, r.definitionName); err != nil {
			return err
		}
		if err := writeString(buf, r.relationName); err != nil {
			return err
		}
		if nonDefault {
			//nolint:gosec  // checkStrategy is a constant iota that will fit in byte
			if _, err := buf.Write([]byte{byte(r.checkStrategy)}); err != nil {
				return err
			}
		}
		return r.templateTree.Serialize(buf)
	})
}

func deserializeRecursive(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("recursive flags: %w", err)
	}
	defName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("recursive def: %w", err)
	}
	relName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("recursive rel: %w", err)
	}
	strategy := recursiveCheckIterSubjects
	if hasFlag(flags, recursiveFlagStrategy) {
		b, err := br.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("recursive strategy: %w", err)
		}
		strategy = recursiveCheckStrategy(b)
	}
	sub, err := Deserialize(br, dctx)
	if err != nil {
		return nil, fmt.Errorf("recursive template: %w", err)
	}
	ri := NewRecursiveIterator(sub, defName, relName)
	ri.checkStrategy = strategy
	ri.canonicalKey = key
	return ri, nil
}
