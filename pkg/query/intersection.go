package query

import (
	"github.com/authzed/spicedb/pkg/genutil/slicez"
)

// Intersection the set of relations that are in all of underlying subiterators.
// This is equivalent to `permission foo = bar & baz`
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

func (i *Intersection) Check(ctx *Context, resourceIDs []string, subjectID string) (RelationSeq, error) {
	valid := resourceIDs

	var rels []Relation

	for _, it := range i.subIts {
		relSeq, err := it.Check(ctx, valid, subjectID)
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

func (i *Intersection) LookupSubjects(ctx *Context, resourceID string) (RelationSeq, error) {
	return nil, ErrUnimplemented
}

func (i *Intersection) LookupResources(ctx *Context, subjectID string) (RelationSeq, error) {
	return nil, ErrUnimplemented
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
