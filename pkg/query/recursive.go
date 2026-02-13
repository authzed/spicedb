package query

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultMaxRecursionDepth = 50

// recursiveCheckStrategy specifies which strategy to use for Check operations
type recursiveCheckStrategy int

const (
	// recursiveCheckIterSubjects calls IterSubjects for each resource, filters by subject
	recursiveCheckIterSubjects recursiveCheckStrategy = iota
	// recursiveCheckIterResources calls IterResources with subject, filters by resources
	recursiveCheckIterResources
	// recursiveCheckDeepening uses iterative deepening (current implementation)
	recursiveCheckDeepening
)

var _ Iterator = &RecursiveIterator{}

// RecursiveIterator is the root controller that manages iterative deepening for recursive schemas.
// It wraps an iterator tree that contains RecursiveSentinel sentinels, and executes the tree
// repeatedly with increasing depth until a fixed point is reached or max depth is exceeded.
type RecursiveIterator struct {
	id             string
	templateTree   Iterator
	definitionName string                 // The schema definition this iterator is recursing on
	relationName   string                 // The relation name this iterator is recursing on
	checkStrategy  recursiveCheckStrategy // strategy for Check operations
	canonicalKey   CanonicalKey
}

// NewRecursiveIterator creates a new recursive iterator controller
func NewRecursiveIterator(templateTree Iterator, definitionName, relationName string) *RecursiveIterator {
	return &RecursiveIterator{
		id:             uuid.NewString(),
		templateTree:   templateTree,
		definitionName: definitionName,
		relationName:   relationName,
		checkStrategy:  recursiveCheckIterSubjects, // default strategy
	}
}

// findMatchingSentinels walks the template tree and returns IDs of sentinels that match
// this RecursiveIterator's definition and relation (but stops at nested RecursiveIterators).
func (r *RecursiveIterator) findMatchingSentinels() []string {
	var sentinelIDs []string
	_, _ = Walk(r.templateTree, func(it Iterator) (Iterator, error) {
		// Stop traversing if we encounter a nested RecursiveIterator
		if _, isRecursive := it.(*RecursiveIterator); isRecursive {
			return it, nil // Don't traverse into nested RecursiveIterators
		}

		// Collect matching sentinels
		if sentinel, ok := it.(*RecursiveSentinelIterator); ok {
			if sentinel.DefinitionName() == r.definitionName &&
				sentinel.RelationName() == r.relationName {
				sentinelIDs = append(sentinelIDs, sentinel.ID())
			}
		}
		return it, nil
	})
	return sentinelIDs
}

// CheckImpl implements traversal for Check operations with strategy selection
func (r *RecursiveIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	switch r.checkStrategy {
	case recursiveCheckIterSubjects:
		return r.recursiveCheckIterSubjects(ctx, resources, subject)
	case recursiveCheckIterResources:
		return r.recursiveCheckIterResources(ctx, resources, subject)
	case recursiveCheckDeepening:
		return r.deepeningCheck(ctx, resources, subject)
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
		id:             uuid.NewString(),
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
		id:             uuid.NewString(),
		templateTree:   newSubs[0],
		definitionName: r.definitionName,
		relationName:   r.relationName,
		checkStrategy:  r.checkStrategy, // preserve strategy
	}, nil
}

func (r *RecursiveIterator) ID() string {
	return r.id
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
	ctx.TraceStep(r, "BFS IterSubjects: resource=%s:%s, filter=%s",
		resource.ObjectType, resource.ObjectID, filterSubjectType.Type)

	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	// Find all matching sentinels in the template tree
	sentinelIDs := r.findMatchingSentinels()
	ctx.TraceStep(r, "Found %d matching sentinels: %v", len(sentinelIDs), sentinelIDs)

	return func(yield func(Path, error) bool) {
		// Track yielded paths by endpoints (for deduplication with OR semantics)
		yieldedPaths := make(map[string]Path)

		// Track queried objects to prevent cycles (avoid re-querying same objects)
		queriedObjects := make(map[string]bool)

		// Frontier: paths representing the "wave front" of exploration
		// At each ply, we query IterSubjects FROM the subjects of these paths
		var frontierPaths []Path

		// Ply 0: Start with a seed path representing the initial resource
		seedPath := Path{
			Resource: resource,
			Relation: tuple.Ellipsis, // Ellipsis represents "identity"
			Subject: ObjectAndRelation{
				ObjectType: resource.ObjectType,
				ObjectID:   resource.ObjectID,
				Relation:   tuple.Ellipsis,
			},
		}
		frontierPaths = []Path{seedPath}
		queriedObjects[resource.Key()] = true

		for ply := 0; ply < maxDepth; ply++ {
			ctx.TraceStep(r, "Ply %d: %d frontier paths", ply, len(frontierPaths))

			// Enable collection mode for all matching sentinels
			for _, sentinelID := range sentinelIDs {
				ctx.EnableFrontierCollection(sentinelID)
			}

			// Track paths collected at this ply (keyed by endpoints for deduplication)
			plyPaths := make(map[string]Path)

			// Query IterSubjects FROM each frontier object
			for _, frontierPath := range frontierPaths {
				// Extract the frontier object from the path's subject
				frontierResource := GetObject(frontierPath.Subject)

				ctx.TraceStep(r, "Ply %d: querying from %s:%s",
					ply, frontierResource.ObjectType, frontierResource.ObjectID)

				// Query IterSubjects with this frontier resource
				// Use NoObjectFilter to get both target subjects AND recursive subjects
				subSeq, err := ctx.IterSubjects(r.templateTree, frontierResource, NoObjectFilter())
				if err != nil {
					yield(Path{}, fmt.Errorf("execution failed at ply %d: %w", ply, err))
					return
				}

				// Collect paths from this frontier node
				for subPath, err := range subSeq {
					if err != nil {
						yield(Path{}, fmt.Errorf("execution failed at ply %d: %w", ply, err))
						return
					}

					// Combine frontier path with new sub-path to get full path from original resource
					// frontierPath: original_resource → frontier_resource
					// subPath:      frontier_resource → subject
					// combined:     original_resource → subject
					combinedPath := Path{
						Resource: resource, // Keep original resource
						Relation: r.relationName,
						Subject:  subPath.Subject,
						// Combine caveats with AND semantics (both must be satisfied)
						Caveat: caveats.And(frontierPath.Caveat, subPath.Caveat),
						// Combine other metadata
						Expiration: combineExpiration(frontierPath.Expiration, subPath.Expiration),
						Integrity:  combineIntegrity(frontierPath.Integrity, subPath.Integrity),
					}

					// Deduplicate by endpoints within this ply
					key := combinedPath.EndpointsKey()
					if existing, seen := plyPaths[key]; seen {
						// Merge with OR semantics (same endpoint, different paths)
						merged, err := existing.MergeOr(combinedPath)
						if err != nil {
							yield(Path{}, err)
							return
						}
						plyPaths[key] = merged
					} else {
						plyPaths[key] = combinedPath
					}
				}
			}

			ctx.TraceStep(r, "Ply %d: found %d unique paths", ply, len(plyPaths))

			// Extract frontier objects collected by all sentinels during this ply
			var collectedObjects []Object
			for _, sentinelID := range sentinelIDs {
				objects := ctx.ExtractFrontierCollection(sentinelID)
				collectedObjects = append(collectedObjects, objects...)
			}
			ctx.TraceStep(r, "Ply %d: collected %d recursive objects", ply, len(collectedObjects))

			// Process collected paths: yield matching ones, prepare frontier for next ply
			var newFrontierPaths []Path
			var yieldedCount int

			for key, path := range plyPaths {
				// Check if this is a recursive subject (needs further exploration)
				isRecursive := path.Subject.ObjectType == r.definitionName

				// Check if this matches the target filter
				matchesFilter := filterSubjectType.Type == "" ||
					path.Subject.ObjectType == filterSubjectType.Type

				// Deduplicate globally
				if existing, seen := yieldedPaths[key]; seen {
					// Already yielded - merge and update
					merged, err := existing.MergeOr(path)
					if err != nil {
						yield(Path{}, err)
						return
					}
					yieldedPaths[key] = merged
				} else {
					// New path globally
					yieldedPaths[key] = path

					// Yield if it matches the filter
					if matchesFilter {
						yieldedCount++
						if !yield(path, nil) {
							return
						}
					}
				}

				// Add to next frontier if recursive (will be explored next ply)
				if isRecursive {
					// Check if we've already queried this object (cycle detection)
					objKey := GetObject(path.Subject).Key()
					if !queriedObjects[objKey] {
						queriedObjects[objKey] = true
						newFrontierPaths = append(newFrontierPaths, path)
						ctx.TraceStep(r, "Ply %d: adding %s to next frontier", ply, objKey)
					} else {
						ctx.TraceStep(r, "Ply %d: skipping %s (already queried, cycle detected)", ply, objKey)
					}
				}
			}

			// Add collected objects to the frontier (these need recursive expansion in next ply)
			for _, obj := range collectedObjects {
				// Check if we've already queried this object (cycle detection)
				objKey := obj.Key()
				if !queriedObjects[objKey] {
					queriedObjects[objKey] = true

					// Create a frontier path for this collected object
					frontierPath := Path{
						Resource: resource,
						Relation: r.relationName,
						Subject: ObjectAndRelation{
							ObjectType: obj.ObjectType,
							ObjectID:   obj.ObjectID,
							Relation:   tuple.Ellipsis,
						},
					}
					newFrontierPaths = append(newFrontierPaths, frontierPath)
					ctx.TraceStep(r, "Ply %d: adding collected object %s to frontier", ply, objKey)
				} else {
					ctx.TraceStep(r, "Ply %d: skipping collected object %s (already queried, cycle detected)", ply, objKey)
				}
			}

			ctx.TraceStep(r, "Ply %d: yielded %d matching paths, %d for next frontier",
				ply, yieldedCount, len(newFrontierPaths))

			// If no recursive paths to expand, we're done
			if len(newFrontierPaths) == 0 {
				ctx.TraceStep(r, "BFS completed (no frontier at ply %d)", ply)
				return
			}

			// Update frontier for next ply
			frontierPaths = newFrontierPaths
		}

		ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
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
	ctx.TraceStep(r, "BFS IterResources with constant subject %s:%s#%s",
		subject.ObjectType, subject.ObjectID, subject.Relation)

	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	return func(yield func(Path, error) bool) {
		// Track all paths yielded (for deduplication)
		yieldedPaths := make(map[string]Path)

		// Current frontier: all paths from previous ply
		var frontierPaths []Path

		// Start with the original tree (sentinel returns empty at ply 0)
		currentTree := r.templateTree

		for ply := 0; ply < maxDepth; ply++ {
			ctx.TraceStep(r, "Ply %d: querying with %d frontier paths", ply, len(frontierPaths))

			// Query IterResources with the ORIGINAL subject
			plySeq, err := ctx.IterResources(currentTree, subject, filterResourceType)
			if err != nil {
				yield(Path{}, err)
				return
			}

			// Collect paths from this ply
			var newPaths []Path
			for path, err := range plySeq {
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Deduplicate by endpoint
				key := path.EndpointsKey()
				if existing, seen := yieldedPaths[key]; seen {
					// Merge with OR semantics
					merged, err := existing.MergeOr(path)
					if err != nil {
						yield(Path{}, err)
						return
					}
					yieldedPaths[key] = merged
					// Don't yield again, but update frontier
					newPaths = append(newPaths, merged)
				} else {
					// New path - yield and add to frontier
					yieldedPaths[key] = path
					newPaths = append(newPaths, path)
					if !yield(path, nil) {
						return
					}
				}
			}

			ctx.TraceStep(r, "Ply %d: found %d new paths", ply, len(newPaths))

			// If no new paths, we're done
			if len(newPaths) == 0 {
				ctx.TraceStep(r, "BFS completed (no new paths at ply %d)", ply)
				return
			}

			// Prepare for next ply: clone tree and replace sentinel with Fixed(frontier)
			frontierPaths = newPaths // Use ALL new paths as frontier
			fixedFrontier := NewFixedIterator(frontierPaths...)

			// Clone tree with sentinel replaced by Fixed frontier
			modifiedTree, err := r.replaceRecursiveSentinel(r.templateTree, fixedFrontier)
			if err != nil {
				yield(Path{}, fmt.Errorf("failed to replace sentinel: %w", err))
				return
			}
			currentTree = modifiedTree
		}

		ctx.TraceStep(r, "BFS terminated at max depth %d", maxDepth)
	}, nil
}

// deepeningCheck implements a deepening traversal for Check operations.
// Unlike IterResources which builds a frontier of paths, deepeningCheck uses iterative deepening
// with early termination: at each ply, we allow one more level of recursion through the
// sentinel by replacing it with progressively deeper trees.
func (r *RecursiveIterator) deepeningCheck(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	maxDepth := ctx.MaxRecursionDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxRecursionDepth
	}

	return func(yield func(Path, error) bool) {
		// Track all paths yielded globally (for deduplication)
		yieldedPaths := make(map[string]bool)
		foundPathsAtPreviousPly := false

		for ply := 0; ply < maxDepth; ply++ {
			ctx.TraceStep(r, "BFS Check: Ply %d starting", ply)

			// Build tree for this ply by replacing sentinel with ply-depth tree
			// At ply 0: sentinel returns empty (no recursion)
			// At ply 1: sentinel replaced with depth-0 tree (1 level of recursion)
			// At ply 2: sentinel replaced with depth-1 tree (2 levels of recursion)
			// Etc.
			plyTree, err := r.buildTreeAtDepth(ply)
			if err != nil {
				yield(Path{}, fmt.Errorf("failed to build tree at ply %d: %w", ply, err))
				return
			}

			// Execute Check with the ply tree
			plySeq, err := ctx.Check(plyTree, resources, subject)
			if err != nil {
				yield(Path{}, fmt.Errorf("check failed at ply %d: %w", ply, err))
				return
			}

			// Collect and deduplicate paths from this ply
			newPathCount := 0
			for path, err := range plySeq {
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Deduplicate by full path key
				key := path.Key()
				if !yieldedPaths[key] {
					yieldedPaths[key] = true
					newPathCount++
					if !yield(path, nil) {
						return
					}
				}
			}

			ctx.TraceStep(r, "BFS Check: Ply %d found %d new paths", ply, newPathCount)

			// Early termination: if we previously found paths but now found no new paths,
			// we've reached a fixed point (all reachable paths have been discovered)
			if newPathCount == 0 && foundPathsAtPreviousPly {
				ctx.TraceStep(r, "BFS Check: Terminated at ply %d (no new paths, fixed point reached)", ply)
				return
			}

			if newPathCount > 0 {
				foundPathsAtPreviousPly = true
			}
		}

		ctx.TraceStep(r, "BFS Check: Reached max depth %d", maxDepth)
	}, nil
}

// recursiveCheckIterSubjects implements Check by calling IterSubjects for each resource
// and filtering paths to match the input subject.
func (r *RecursiveIterator) recursiveCheckIterSubjects(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		// Get subject type for filtering (type only, not relation - ellipsis is not a real relation)
		filterSubjectType := ObjectType{Type: subject.ObjectType}

		pathCount := 0

		// For each input resource, iterate its subjects using BFS
		for _, resource := range resources {
			ctx.TraceStep(r, "Check via IterSubjects: processing resource %s:%s",
				resource.ObjectType, resource.ObjectID)

			// Call IterSubjects on the RecursiveIterator itself - this will use BFS
			// which properly handles the frontier as Path objects
			pathSeq, err := ctx.IterSubjects(r, resource, filterSubjectType)
			if err != nil {
				yield(Path{}, fmt.Errorf("IterSubjects failed for resource %s:%s: %w",
					resource.ObjectType, resource.ObjectID, err))
				return
			}

			// Filter paths where subject matches input subject (compare only type and ID, not relation)
			for path, err := range pathSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Check if path's subject matches the input subject (type and ID only)
				if GetObject(path.Subject).Equals(GetObject(subject)) {
					ctx.TraceStep(r, "Check via IterSubjects: found matching path")
					pathCount++
					if !yield(path, nil) {
						return
					}
				}
			}
		}

		ctx.TraceStep(r, "Check via IterSubjects: completed with %d paths", pathCount)
	}, nil
}

// recursiveCheckIterResources implements Check by calling IterResources with the subject
// and filtering paths to match the input resources.
func (r *RecursiveIterator) recursiveCheckIterResources(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		// Determine filter type from first resource (all should be same type)
		var filterResourceType ObjectType
		if len(resources) > 0 {
			filterResourceType = ObjectType{Type: resources[0].ObjectType}
		}

		pathCount := 0

		ctx.TraceStep(r, "Check via IterResources: processing subject %s:%s#%s",
			subject.ObjectType, subject.ObjectID, subject.Relation)

		// Call IterResources on the RecursiveIterator itself - this will use BFS
		// which properly handles the frontier as Path objects
		pathSeq, err := ctx.IterResources(r, subject, filterResourceType)
		if err != nil {
			yield(Path{}, fmt.Errorf("IterResources failed for subject %s: %w",
				subject.String(), err))
			return
		}

		// Filter paths where resource matches one of input resources
		for path, err := range pathSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}

			// Check if path's resource matches any of the input resources
			for _, resource := range resources {
				if path.Resource.Equals(resource) {
					ctx.TraceStep(r, "Check via IterResources: found matching path from %s to %s",
						path.Resource.Key(), path.Subject.String())
					pathCount++
					if !yield(path, nil) {
						return
					}
					break // Found matching resource, move to next path
				}
			}
		}

		ctx.TraceStep(r, "Check via IterResources: completed with %d paths", pathCount)
	}, nil
}
