package query

import (
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

func (i *Intersection) Check(ctx *Context, resourceIds []string, subjectIds string) (RelationSeq, error) {
	valid := resourceIds

	var rels []Relation

	for _, it := range i.subIts {
		relSeq, err := it.Check(ctx, valid, subjectIds)
		if err != nil {
			return nil, err
		}
		rels, err = CollectAll(relSeq)
		if err != nil {
			return nil, err
		}

		if len(rels) == 0 {
			return nil, nil
		}

		valid = slicez.Map(rels, func(r Relation) string {
			return r.Resource.ObjectID
		})
	}

	return func(yield func(Relation, error) bool) {
		for _, rel := range rels {
			if !yield(rel, nil) {
				return
			}
		}
	}, nil
}

func (i *Intersection) LookupSubjects(ctx *Context, resourceId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
}

func (i *Intersection) LookupResources(ctx *Context, subjectId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
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
	var subs []Explain
	for _, it := range i.subIts {
		subs = append(subs, it.Explain())
	}
	return Explain{
		Info:       "Intersection",
		SubExplain: subs,
	}
}
