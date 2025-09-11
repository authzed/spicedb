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

func NewIntersection() *Intersection {
	return &Intersection{}
}

func (i *Intersection) addSubIterator(subIt Iterator) {
	i.subIts = append(i.subIts, subIt)
}

func (i *Intersection) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	validResources := resources

	// Track paths by resource key for combining with AND logic
	pathsByKey := make(map[string]*Path)

	for iterIdx, it := range i.subIts {
		pathSeq, err := it.CheckImpl(ctx, validResources, subject)
		if err != nil {
			return nil, err
		}
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		if len(paths) == 0 {
			return func(yield func(*Path, error) bool) {}, nil
		}

		if iterIdx == 0 {
			// First iterator - initialize pathsByKey
			for _, path := range paths {
				key := path.Resource.ObjectType + ":" + path.Resource.ObjectID + "#" + path.Relation
				pathsByKey[key] = path
			}
		} else {
			// Subsequent iterators - intersect and combine caveats
			newPathsByKey := make(map[string]*Path)
			for _, path := range paths {
				key := path.Resource.ObjectType + ":" + path.Resource.ObjectID + "#" + path.Relation
				if existing, exists := pathsByKey[key]; exists {
					// Combine caveats using intersection logic (AND)
					combined := *existing  // Copy the existing path
					if err := combined.MergeAnd(path); err != nil {
						return nil, err
					}
					newPathsByKey[key] = &combined
				}
				// If path not in previous results, it's filtered out (intersection)
			}
			pathsByKey = newPathsByKey

			if len(pathsByKey) == 0 {
				return func(yield func(*Path, error) bool) {}, nil
			}
		}

		// Update valid resources for next iteration
		validResources = make([]Object, 0, len(pathsByKey))
		for _, path := range pathsByKey {
			validResources = append(validResources, path.Resource)
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
		Info:       "Intersection",
		SubExplain: subs,
	}
}
