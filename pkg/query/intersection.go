package query

import (
	"iter"

	"github.com/authzed/spicedb/pkg/genutil/slicez"
)

type Intersection struct {
	subIts []Iterator
}

var _ Iterator = &Intersection{}

func NewIntersection() *Intersection {
	return &Intersection{}
}

func (i *Intersection) AddSubIterator(subIt Iterator) {
	i.subIts = append(i.subIts, subIt)
}

func (i *Intersection) Check(ctx Context, resource_ids []string, subject_id string) ([]Relation, error) {
	valid := resource_ids

	var rels []Relation
	var err error

	for _, it := range i.subIts {
		rels, err = it.Check(ctx, valid, subject_id)
		if err != nil {
			return nil, err
		}

		if len(rels) == 0 {
			return nil, nil
		}

		valid = slicez.Map(rels, func(r Relation) string {
			return r.ResourceID
		})
	}

	return rels, nil
}

func (i *Intersection) LookupSubjects(ctx Context, resource_id string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (i *Intersection) LookupResources(ctx Context, subject_id string) (iter.Seq2[Relation, error], error) {
	panic("not implemented") // TODO: Implement
}

func (i *Intersection) Explain() Explain {
	var subs []Explain
	for _, it := range i.subIts {
		subs = append(subs, it.Explain())
	}
	return Explain{
		Info:       "Intersection",
		SubExplain: subs,
	}
}
