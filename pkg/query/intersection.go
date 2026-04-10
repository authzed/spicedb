package query

import (
	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
)

// IntersectionIterator the set of paths that are in all of underlying subiterators.
// This is equivalent to `permission foo = bar & baz`
type IntersectionIterator struct {
	subIts       []Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &IntersectionIterator{}

func NewIntersectionIterator(subiterators ...Iterator) Iterator {
	if len(subiterators) == 0 {
		return NewFixedIterator() // Return empty FixedIterator instead of empty Intersection
	}
	return &IntersectionIterator{
		subIts: subiterators,
	}
}

func (i *IntersectionIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(i, "processing %d sub-iterators for resource %s:%s", len(i.subIts), resource.ObjectType, resource.ObjectID)
	}

	var result *Path
	for iterIdx, it := range i.subIts {
		if ctx.shouldTrace() {
			ctx.TraceStep(i, "processing sub-iterator %d", iterIdx)
		}

		path, err := ctx.Check(it, resource, subject)
		if err != nil {
			return nil, err
		}

		if path == nil {
			if ctx.shouldTrace() {
				ctx.TraceStep(i, "sub-iterator %d returned nil, short-circuiting", iterIdx)
			}
			return nil, nil
		}

		result, err = result.MergeAnd(path)
		if err != nil {
			return nil, err
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(i, "sub-iterator %d matched", iterIdx)
		}
	}

	return result, nil
}

func (i *IntersectionIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(i, "iterating subjects for resource %s:%s from %d sub-iterators", resource.ObjectType, resource.ObjectID, len(i.subIts))
	}

	// Track concrete paths by subject key and a separate wildcard path.
	// A wildcard (subject ID = "*") acts as a universal set that intersects with any concrete subject.
	pathsByKey := make(map[string]*Path)
	var wildcardPath *Path

	for iterIdx, it := range i.subIts {
		if ctx.shouldTrace() {
			ctx.TraceStep(i, "processing sub-iterator %d", iterIdx)
		}

		pathSeq, err := ctx.IterSubjects(it, resource, filterSubjectType)
		if err != nil {
			return nil, err
		}
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(i, "sub-iterator %d returned %d paths", iterIdx, len(paths))
		}

		if len(paths) == 0 {
			if ctx.shouldTrace() {
				ctx.TraceStep(i, "sub-iterator %d returned empty, short-circuiting", iterIdx)
			}
			return EmptyPathSeq(), nil
		}

		// Separate wildcard from concrete paths in this iterator's results.
		var currentWildcard *Path
		currentIterPaths := make(map[string]*Path)
		for _, path := range paths {
			if path.Subject.ObjectID == tuple.PublicWildcard {
				if currentWildcard == nil {
					wc := *path
					currentWildcard = &wc
				} else {
					if _, err := currentWildcard.MergeOr(path); err != nil {
						return nil, err
					}
				}
				continue
			}

			key := ObjectAndRelationKey(path.Subject)
			if existing, exists := currentIterPaths[key]; !exists {
				pathCopy := *path
				currentIterPaths[key] = &pathCopy
			} else {
				if _, err := existing.MergeOr(path); err != nil {
					return nil, err
				}
			}
		}

		if iterIdx == 0 {
			pathsByKey = currentIterPaths
			wildcardPath = currentWildcard
		} else {
			pathsByKey, wildcardPath, err = intersectSubjectSets(pathsByKey, wildcardPath, currentIterPaths, currentWildcard)
			if err != nil {
				return nil, err
			}

			if len(pathsByKey) == 0 && wildcardPath == nil {
				return EmptyPathSeq(), nil
			}
		}
	}

	return func(yield func(*Path, error) bool) {
		if wildcardPath != nil {
			if !yield(wildcardPath, nil) {
				return
			}
		}
		for _, path := range pathsByKey {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

// intersectSubjectSets performs a wildcard-aware intersection of two subject sets.
// A wildcard (subject ID = "*") acts as a universal: it matches all concrete subjects.
//
// The intersection rules are:
//   - Concrete ∩ concrete: keep subjects present in both (caveats AND'd)
//   - Concrete ∩ wildcard: keep the concrete subject (wildcard's caveat AND'd in)
//   - Wildcard ∩ wildcard: keep wildcard (caveats AND'd)
func intersectSubjectSets(
	prevConcrete map[string]*Path, prevWildcard *Path,
	currConcrete map[string]*Path, currWildcard *Path,
) (map[string]*Path, *Path, error) {
	result := make(map[string]*Path)

	// Concrete subjects that exist in both sides.
	for key, currPath := range currConcrete {
		if prevPath, exists := prevConcrete[key]; exists {
			merged := *prevPath
			if _, err := merged.MergeAnd(currPath); err != nil {
				return nil, nil, err
			}
			result[key] = &merged
		}
	}

	// Concrete subjects in prev matched by curr's wildcard.
	if currWildcard != nil {
		for key, prevPath := range prevConcrete {
			if _, exists := result[key]; exists {
				continue // already handled by concrete ∩ concrete
			}
			synth := *prevPath
			synth.Caveat = caveats.And(prevPath.Caveat, currWildcard.Caveat)
			result[key] = &synth
		}
	}

	// Concrete subjects in curr matched by prev's wildcard.
	if prevWildcard != nil {
		for key, currPath := range currConcrete {
			if _, exists := result[key]; exists {
				continue // already handled above
			}
			synth := *currPath
			synth.Caveat = caveats.And(currPath.Caveat, prevWildcard.Caveat)
			result[key] = &synth
		}
	}

	// Wildcard ∩ wildcard → wildcard with AND'd caveats.
	var resultWildcard *Path
	if prevWildcard != nil && currWildcard != nil {
		wc := *prevWildcard
		wc.Caveat = caveats.And(prevWildcard.Caveat, currWildcard.Caveat)
		resultWildcard = &wc
	}

	return result, resultWildcard, nil
}

func (i *IntersectionIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(i, "iterating resources for subject %s:%s from %d sub-iterators", subject.ObjectType, subject.ObjectID, len(i.subIts))
	}

	// Track paths by resource key for combining with AND logic
	pathsByKey := make(map[string]*Path)

	for iterIdx, it := range i.subIts {
		if ctx.shouldTrace() {
			ctx.TraceStep(i, "processing sub-iterator %d", iterIdx)
		}

		pathSeq, err := ctx.IterResources(it, subject, filterResourceType)
		if err != nil {
			return nil, err
		}
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(i, "sub-iterator %d returned %d paths", iterIdx, len(paths))
		}

		if len(paths) == 0 {
			if ctx.shouldTrace() {
				ctx.TraceStep(i, "sub-iterator %d returned empty, short-circuiting", iterIdx)
			}
			return EmptyPathSeq(), nil
		}

		if iterIdx == 0 {
			// First iterator - initialize pathsByKey using resource-based keys
			for _, path := range paths {
				key := path.Resource.Key()
				if existing, exists := pathsByKey[key]; !exists {
					pathCopy := *path
					pathsByKey[key] = &pathCopy
				} else {
					// Only merge paths with matching subjects
					if !GetObject(existing.Subject).Equals(GetObject(path.Subject)) {
						// Keep the first one, skip others with different subjects
						continue
					}

					// If multiple paths for same resource in first iterator, merge with OR (mutates existing)
					if _, err := existing.MergeOr(path); err != nil {
						return nil, err
					}
				}
			}
		} else {
			// Subsequent iterators - intersect based on resources and combine caveats
			newPathsByKey := make(map[string]*Path)

			// First collect all paths from this iterator by resource
			currentIterPaths := make(map[string]*Path)
			for _, path := range paths {
				key := path.Resource.Key()
				if existing, exists := currentIterPaths[key]; !exists {
					pathCopy := *path
					currentIterPaths[key] = &pathCopy
				} else {
					// Only merge paths with matching subjects
					if !GetObject(existing.Subject).Equals(GetObject(path.Subject)) {
						// Keep the first one, skip others with different subjects
						continue
					}

					// Multiple paths for same resource in current iterator, merge with OR (mutates existing)
					if _, err := existing.MergeOr(path); err != nil {
						return nil, err
					}
				}
			}

			// Now intersect: only keep subjects that exist in both previous and current
			for key, currentPath := range currentIterPaths {
				if existing, exists := pathsByKey[key]; exists {
					// Only merge paths with matching subjects
					// Paths with different subjects can't be combined with AND logic
					if !GetObject(existing.Subject).Equals(GetObject(currentPath.Subject)) {
						// Subjects don't match - skip this resource
						continue
					}

					// Combine using intersection logic (AND) (mutates existing)
					if _, err := existing.MergeAnd(currentPath); err != nil {
						return nil, err
					}
					newPathsByKey[key] = existing
				}
				// If resource not in previous results, it's filtered out (intersection)
			}
			pathsByKey = newPathsByKey

			if len(pathsByKey) == 0 {
				return EmptyPathSeq(), nil
			}
		}
	}

	return func(yield func(*Path, error) bool) {
		for _, path := range pathsByKey {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (i *IntersectionIterator) Clone() Iterator {
	cloned := &IntersectionIterator{
		canonicalKey: i.canonicalKey,
		subIts:       make([]Iterator, len(i.subIts)),
	}
	for idx, subIt := range i.subIts {
		cloned.subIts[idx] = subIt.Clone()
	}
	return cloned
}

func (i *IntersectionIterator) Explain() Explain {
	subs := make([]Explain, len(i.subIts))
	for i, it := range i.subIts {
		subs[i] = it.Explain()
	}
	return Explain{
		Name:       "Intersection",
		Info:       "Intersection",
		SubExplain: subs,
	}
}

func (i *IntersectionIterator) Subiterators() []Iterator {
	return i.subIts
}

func (i *IntersectionIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &IntersectionIterator{canonicalKey: i.canonicalKey, subIts: newSubs}, nil
}

func (i *IntersectionIterator) CanonicalKey() CanonicalKey {
	return i.canonicalKey
}

func (i *IntersectionIterator) ResourceType() ([]ObjectType, error) {
	if len(i.subIts) == 0 {
		return []ObjectType{}, nil
	}

	// For intersection, return types that are common to all sub-iterators
	// Start with types from the first iterator
	firstTypes, err := i.subIts[0].ResourceType()
	if err != nil {
		return nil, err
	}

	if len(i.subIts) == 1 {
		return firstTypes, nil
	}

	// Build a set of types that appear in ALL sub-iterators
	result := mapz.NewSet(firstTypes...)

	// Intersect with each subsequent iterator's types
	for _, subIt := range i.subIts[1:] {
		subTypes, err := subIt.ResourceType()
		if err != nil {
			return nil, err
		}

		subSet := mapz.NewSet(subTypes...)
		result = result.Intersect(subSet)
	}

	return result.AsSlice(), nil
}

func (i *IntersectionIterator) SubjectTypes() ([]ObjectType, error) {
	return collectAndDeduplicateSubjectTypes(i.subIts)
}
