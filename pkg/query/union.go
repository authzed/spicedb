package query

import (
	"slices"
)

type Union struct {
	subIts []Iterator
}

var _ Iterator = &Union{}

func NewUnion() *Union {
	return &Union{}
}

func (u *Union) AddSubIterator(subIt Iterator) {
	u.subIts = append(u.subIts, subIt)
}

func (u *Union) Check(ctx *Context, resourceIds []string, subjectIds string) (RelationSeq, error) {
	remaining := resourceIds
	var out []Relation
	for _, it := range u.subIts {
		relSeq, err := it.Check(ctx, remaining, subjectIds)
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
			if idx := slices.Index(remaining, r.Resource.ObjectID); idx != -1 {
				remaining = slices.Delete(remaining, idx, idx+1)
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

func (u *Union) LookupSubjects(ctx *Context, resourceId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
}

func (u *Union) LookupResources(ctx *Context, subjectId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
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
	var subs []Explain
	for _, it := range u.subIts {
		subs = append(subs, it.Explain())
	}
	return Explain{
		Info:       "Union",
		SubExplain: subs,
	}
}
