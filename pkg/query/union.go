package query

import (
	"slices"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Union the set of relations that are in any of underlying subiterators.
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

func (u *Union) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	remaining := make([]Object, len(resources))
	copy(remaining, resources)
	var out []Relation
	for _, it := range u.subIts {
		relSeq, err := it.CheckImpl(ctx, remaining, subject)
		if err != nil {
			return nil, err
		}
		rels, err := CollectAll(relSeq)
		if err != nil {
			return nil, err
		}

		out = append(out, rels...)

		// If some subset have already passed the check, no need to check them again.
		for _, r := range rels {
			for idx, res := range remaining {
				if res.ObjectID == r.Resource.ObjectID && res.ObjectType == r.Resource.ObjectType {
					remaining = slices.Delete(remaining, idx, idx+1)
					break
				}
			}
		}

		if len(remaining) == 0 {
			break
		}
	}
	return func(yield func(Relation, error) bool) {
		for _, rel := range out {
			if !yield(rel, nil) {
				return
			}
		}
	}, nil
}

func (u *Union) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (u *Union) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
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
