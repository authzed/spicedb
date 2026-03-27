package query

import (
	"github.com/authzed/spicedb/pkg/genutil/mapz"
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

	// Track paths by subject key for combining with AND logic
	pathsByKey := make(map[string]*Path)

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

		if iterIdx == 0 {
			// First iterator - initialize pathsByKey using subject-based keys
			for _, path := range paths {
				key := ObjectAndRelationKey(path.Subject)
				if existing, exists := pathsByKey[key]; !exists {
					pathCopy := *path
					pathsByKey[key] = &pathCopy
				} else {
					// If multiple paths for same subject in first iterator, merge with OR (mutates existing)
					if _, err := existing.MergeOr(path); err != nil {
						return nil, err
					}
				}
			}
		} else {
			// Subsequent iterators - intersect based on subjects and combine caveats
			newPathsByKey := make(map[string]*Path)

			// First collect all paths from this iterator by subject
			currentIterPaths := make(map[string]*Path)
			for _, path := range paths {
				key := ObjectAndRelationKey(path.Subject)
				if existing, exists := currentIterPaths[key]; !exists {
					pathCopy := *path
					currentIterPaths[key] = &pathCopy
				} else {
					// Multiple paths for same subject in current iterator, merge with OR (mutates existing)
					if _, err := existing.MergeOr(path); err != nil {
						return nil, err
					}
				}
			}

			// Now intersect: only keep subjects that exist in both previous and current
			for key, currentPath := range currentIterPaths {
				if existing, exists := pathsByKey[key]; exists {
					// Combine using intersection logic (AND) (mutates existing)
					if _, err := existing.MergeAnd(currentPath); err != nil {
						return nil, err
					}
					newPathsByKey[key] = existing
				}
				// If subject not in previous results, it's filtered out (intersection)
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
