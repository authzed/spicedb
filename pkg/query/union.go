package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Union the set of paths that are in any of underlying subiterators.
// This is equivalent to `permission foo = bar | baz`
type Union struct {
	subIts []Iterator
}

var _ Iterator = &Union{}

func NewUnion(subiterators ...Iterator) *Union {
	if len(subiterators) == 0 {
		return &Union{}
	}
	return &Union{
		subIts: subiterators,
	}
}

func (u *Union) addSubIterator(subIt Iterator) {
	u.subIts = append(u.subIts, subIt)
}

func (u *Union) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	ctx.TraceStep(u, "processing %d sub-iterators with %d resources", len(u.subIts), len(resources))

	// Create a concatenated sequence from all sub-iterators
	combinedSeq := func(yield func(Path, error) bool) {
		for iterIdx, it := range u.subIts {
			ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)

			pathSeq, err := ctx.Check(it, resources, subject)
			if err != nil {
				yield(Path{}, err)
				return
			}

			pathCount := 0
			for path, err := range pathSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				pathCount++
				if !yield(path, nil) {
					return
				}
			}

			ctx.TraceStep(u, "sub-iterator %d returned %d paths", iterIdx, pathCount)
		}
	}

	// Wrap with deduplication
	return DeduplicatePathSeq(combinedSeq), nil
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
		Name:       "Union",
		Info:       "Union",
		SubExplain: subs,
	}
}

func (u *Union) Subiterators() []Iterator {
	return u.subIts
}

func (u *Union) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &Union{subIts: newSubs}, nil
}
