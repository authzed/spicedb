package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Intersection the set of paths that are in all of underlying subiterators.
// This is equivalent to `permission foo = bar & baz`
type Intersection struct {
	subIts []Iterator
}

var _ Iterator = &Intersection{}

func NewIntersection(subiterators ...Iterator) *Intersection {
	if len(subiterators) == 0 {
		return &Intersection{}
	}
	return &Intersection{
		subIts: subiterators,
	}
}

func (i *Intersection) addSubIterator(subIt Iterator) {
	i.subIts = append(i.subIts, subIt)
}

func (i *Intersection) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	validResources := resources

	// Track paths by resource key for combining with AND logic
	pathsByKey := make(map[string]Path)

	for iterIdx, it := range i.subIts {
		ctx.TraceStep(i, "processing sub-iterator %d with %d resources", iterIdx, len(validResources))

		pathSeq, err := ctx.Check(it, validResources, subject)
		if err != nil {
			return nil, err
		}
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		ctx.TraceStep(i, "sub-iterator %d returned %d paths", iterIdx, len(paths))

		if len(paths) == 0 {
			ctx.TraceStep(i, "sub-iterator %d returned empty, short-circuiting", iterIdx)
			return func(yield func(Path, error) bool) {}, nil
		}

		if iterIdx == 0 {
			// First iterator - initialize pathsByKey using endpoint-based keys
			for _, path := range paths {
				key := path.Resource.Key()
				if existing, exists := pathsByKey[key]; !exists {
					pathsByKey[key] = path
				} else {
					// If multiple paths for same endpoint in first iterator, merge with OR
					merged, err := existing.MergeOr(path)
					if err != nil {
						return nil, err
					}
					pathsByKey[key] = merged
				}
			}
		} else {
			// Subsequent iterators - intersect based on endpoints and combine caveats
			newPathsByKey := make(map[string]Path)

			// First collect all paths from this iterator by endpoint
			currentIterPaths := make(map[string]Path)
			for _, path := range paths {
				key := path.Resource.Key()
				if existing, exists := currentIterPaths[key]; !exists {
					currentIterPaths[key] = path
				} else {
					// Multiple paths for same endpoint in current iterator, merge with OR
					merged, err := existing.MergeOr(path)
					if err != nil {
						return nil, err
					}
					currentIterPaths[key] = merged
				}
			}

			// Now intersect: only keep endpoints that exist in both previous and current
			for key, currentPath := range currentIterPaths {
				if existing, exists := pathsByKey[key]; exists {
					// Combine using intersection logic (AND)
					combined, err := existing.MergeAnd(currentPath)
					if err != nil {
						return nil, err
					}
					newPathsByKey[key] = combined
				}
				// If endpoint not in previous results, it's filtered out (intersection)
			}
			pathsByKey = newPathsByKey

			if len(pathsByKey) == 0 {
				return func(yield func(Path, error) bool) {}, nil
			}
		}

		// Update valid resources for next iteration (extract unique resources from paths)
		resourceSet := make(map[string]Object)
		for _, path := range pathsByKey {
			resourceKey := path.Resource.Key()
			resourceSet[resourceKey] = path.Resource
		}
		validResources = make([]Object, 0, len(resourceSet))
		for _, obj := range resourceSet {
			validResources = append(validResources, obj)
		}
	}

	return func(yield func(Path, error) bool) {
		for _, path := range pathsByKey {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (i *Intersection) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (i *Intersection) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (i *Intersection) Clone() Iterator {
	cloned := &Intersection{
		subIts: make([]Iterator, len(i.subIts)),
	}
	for idx, subIt := range i.subIts {
		cloned.subIts[idx] = subIt.Clone()
	}
	return cloned
}

func (i *Intersection) Explain() Explain {
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

func (i *Intersection) Subiterators() []Iterator {
	return i.subIts
}

func (i *Intersection) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &Intersection{subIts: newSubs}, nil
}
