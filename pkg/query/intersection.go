package query

import (
	"github.com/authzed/spicedb/pkg/genutil/slicez"
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

	var paths []Path

	for _, it := range i.subIts {
		pathSeq, err := it.CheckImpl(ctx, validResources, subject)
		if err != nil {
			return nil, err
		}
		paths, err = CollectAll(pathSeq)
		if err != nil {
			return nil, err
		}

		if len(paths) == 0 {
			return func(yield func(Path, error) bool) {}, nil
		}

		validResources = slicez.Map(paths, func(p Path) Object {
			return p.Resource
		})
	}

	return func(yield func(Path, error) bool) {
		for _, path := range paths {
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
