package query

import (
	"iter"
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

func (u *Union) Check(ctx Context, resource_ids []string, subject_id string) ([]Relation, error) {
	remaining := resource_ids
	var out []Relation
	for _, it := range u.subIts {
		rels, err := it.Check(ctx, remaining, subject_id)
		if err != nil {
			return nil, err
		}

		out = append(out, rels...)

		// If some subset have already passed the check, no need to check them again.
		for _, r := range rels {
			if idx := slices.Index(remaining, r.ResourceID); idx != -1 {
				remaining = slices.Delete(remaining, idx, idx+1)
			}
		}

		if len(remaining) == 0 {
			break
		}
	}
	return out, nil
}

func (u *Union) LookupSubjects(ctx Context, resource_id string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (u *Union) LookupResources(ctx Context, subject_id string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
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
