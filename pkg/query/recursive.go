package query

import (
	"fmt"
	"time"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// frontierEntry is a lightweight frontier node for BFS IterSubjects.
// It carries only the fields needed to combine with the next hop's path —
// unlike a full *Path it does not hold Resource, Relation, or Metadata.
type frontierEntry struct {
	Subject    ObjectAndRelation
	Caveat     *core.CaveatExpression
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
		// Track yielded paths by endpoints for global deduplication with OR/caveat semantics.
		yieldedPaths := make(map[string]*Path)

		// Track queried objects to prevent cycles (avoid re-querying same objects).
		queriedObjects := make(map[string]bool)

		// frontier holds lightweight entries — just the fields needed to combine with the next
		// hop's path. Using frontierEntry rather than *Path avoids keeping Resource, Relation,
		// and Metadata alive across plies.
		frontier := []frontierEntry{
			{
				Subject: ObjectAndRelation{
					ObjectType: resource.ObjectType,
					ObjectID:   resource.ObjectID,
					Relation:   tuple.Ellipsis,
				},
			},
		}
		queriedObjects[resource.Key()] = true

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
			// Paths with the same endpoint from different frontier nodes are merged with OR.
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
					combinedPath := &Path{
						Resource:   resource,
						Relation:   r.relationName,
						Subject:    subPath.Subject,
						Caveat:     caveats.And(fe.Caveat, subPath.Caveat),
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

			// Extract frontier objects collected by all sentinels during this ply.
			var collectedObjects []Object
			for _, sentinelID := range sentinelIDs {
				collectedObjects = append(collectedObjects, ctx.ExtractFrontierCollection(sentinelID)...)
			}
			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: collected %d recursive objects", ply, len(collectedObjects))
			}

			// Reset and reuse nextFrontier.
			nextFrontier = nextFrontier[:0]
			yieldedCount := 0

			for key, path := range plyPaths {
				isRecursive := path.Subject.ObjectType == r.definitionName
				matchesFilter := filterSubjectType.Type == "" ||
					path.Subject.ObjectType == filterSubjectType.Type

				if existing, seen := yieldedPaths[key]; seen {
					if _, err := existing.MergeOr(path); err != nil {
						yield(nil, err)
						return
					}
				} else {
					yieldedPaths[key] = path
					if matchesFilter {
						yieldedCount++
						if !yield(path, nil) {
							return
						}
					}
				}

				if isRecursive {
					objKey := GetObject(path.Subject).Key()
					if !queriedObjects[objKey] {
						queriedObjects[objKey] = true
						nextFrontier = append(nextFrontier, frontierEntry{
							Subject:    path.Subject,
							Caveat:     path.Caveat,
							Expiration: path.Expiration,
							Integrity:  path.Integrity,
						})
						if ctx.shouldTrace() {
							ctx.TraceStep(r, "Ply %d: adding %s to next frontier", ply, objKey)
						}
					} else if ctx.shouldTrace() {
						ctx.TraceStep(r, "Ply %d: skipping %s (already queried, cycle detected)", ply, objKey)
					}
				}
			}

			// Add sentinel-collected objects to the frontier.
			for _, obj := range collectedObjects {
				objKey := obj.Key()
				if !queriedObjects[objKey] {
					queriedObjects[objKey] = true
					nextFrontier = append(nextFrontier, frontierEntry{
						Subject: ObjectAndRelation{
							ObjectType: obj.ObjectType,
							ObjectID:   obj.ObjectID,
							Relation:   tuple.Ellipsis,
						},
					})
					if ctx.shouldTrace() {
						ctx.TraceStep(r, "Ply %d: adding collected object %s to frontier", ply, objKey)
					}
				} else if ctx.shouldTrace() {
					ctx.TraceStep(r, "Ply %d: skipping collected object %s (already queried, cycle detected)", ply, objKey)
				}
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: yielded %d matching paths, %d for next frontier",
					ply, yieldedCount, len(nextFrontier))
			}

			if len(nextFrontier) == 0 {
				if ctx.shouldTrace() {
					ctx.TraceStep(r, "BFS completed (no frontier at ply %d)", ply)
				}
				return
			}

			// Swap frontier slices — nextFrontier becomes the active frontier.
			// The old frontier slice is reused as the next nextFrontier buffer.
			frontier, nextFrontier = nextFrontier, frontier[:0]
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
		}
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
		// Track all paths yielded (for deduplication with OR/caveat semantics).
		yieldedPaths := make(map[string]*Path)

		// newPaths collects genuinely new paths each ply; reused across plies.
		var newPaths []*Path

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

			// Reset new-paths accumulator, reusing backing array.
			newPaths = newPaths[:0]

			for path, err := range plySeq {
				if err != nil {
					yield(nil, err)
					return
				}

				// Deduplicate by endpoint.
				key := path.EndpointsKey()
				if existing, seen := yieldedPaths[key]; seen {
					// Already yielded — merge caveats with OR semantics but do NOT
					// re-add to the frontier (it was already queried in a prior ply).
					if _, err := existing.MergeOr(path); err != nil {
						yield(nil, err)
						return
					}
				} else {
					// Genuinely new path — record, yield, and add to frontier.
					pathCopy := *path
					yieldedPaths[key] = &pathCopy
					newPaths = append(newPaths, path)
					if !yield(path, nil) {
						return
					}
				}
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(r, "Ply %d: found %d new paths", ply, len(newPaths))
			}

			// If no genuinely new paths, we've reached a fixed point.
			if len(newPaths) == 0 {
				if ctx.shouldTrace() {
					ctx.TraceStep(r, "BFS completed (no new paths at ply %d)", ply)
				}
				return
			}

			// Build Fixed iterator from new paths only (not already-queried paths).
			derefed := make([]Path, len(newPaths))
			for i, p := range newPaths {
				derefed[i] = *p
			}
			fixedFrontier := NewFixedIterator(derefed...)

			// Replace sentinel with Fixed frontier for next ply.
			modifiedTree, err := r.replaceRecursiveSentinel(r.templateTree, fixedFrontier)
			if err != nil {
				yield(nil, fmt.Errorf("failed to replace sentinel: %w", err))
				return
			}
			currentTree = modifiedTree
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
		}
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
