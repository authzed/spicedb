package query

import (
	"maps"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Union the set of paths that are in any of underlying subiterators.
// This is equivalent to `permission foo = bar | baz`
type Union struct {
	subIts []Iterator
}

var _ Iterator = &Union{}

func NewUnion() *Union {
	return &Union{}
}

func (u *Union) addSubIterator(subIt Iterator) {
	u.subIts = append(u.subIts, subIt)
}

func (u *Union) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	var out []Path
	// Collect paths from all sub-iterators
	for _, it := range u.subIts {
		pathSeq, err := it.CheckImpl(ctx, resources, subject)
		if err != nil {
			return nil, err
		}
		paths, err := CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		out = append(out, paths...)
	}

	// Deduplicate paths based on resource for CheckImpl
	// Since the subject is fixed in CheckImpl, we only need to deduplicate by resource
	seen := make(map[string]Path)
	for _, path := range out {
		// Use resource object (type + id) as key for deduplication, not the full resource with relation
		key := path.Resource.ObjectType + ":" + path.Resource.ObjectID
		if existing, exists := seen[key]; !exists {
			seen[key] = path
		} else {
			// If we already have a path for this resource,
			// merge it with the new one using OR semantics
			merged, err := existing.MergeOr(path)
			if err != nil {
				return nil, err
			}
			seen[key] = merged
		}
	}

	// Convert map to slice
	deduplicated := maps.Values(seen)

	return func(yield func(Path, error) bool) {
		for path := range deduplicated {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (u *Union) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (u *Union) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (u *Union) Clone() Iterator {
	cloned := &Union{
		subIts: make([]Iterator, len(u.subIts)),
	}
	for idx, subIt := range u.subIts {
		cloned.subIts[idx] = subIt.Clone()
	}
	return cloned
}

func (u *Union) Explain() Explain {
	subs := make([]Explain, len(u.subIts))
	for i, it := range u.subIts {
		subs[i] = it.Explain()
	}
	return Explain{
		Info:       "Union",
		SubExplain: subs,
	}
}
