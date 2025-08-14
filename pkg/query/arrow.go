package query

import (
	"github.com/authzed/spicedb/pkg/tuple"
)

type Arrow struct {
	left  Iterator
	right Iterator
}

var _ Iterator = &Arrow{}

func NewArrow(left, right Iterator) *Arrow {
	return &Arrow{
		left:  left,
		right: right,
	}
}

func (a *Arrow) Check(ctx *Context, resourceIds []string, subjectId string) (RelationSeq, error) {
	// TODO -- the ordering, directionality, batching, everything can depend on other statistics.
	//
	// There are three major strategies:
	// - LookupSubjects on the left, Check on the right (as per this implementation)
	// - LookupResources on the right, Check on the left
	// - LookupSubjects on left, LookupResources on right, and intersect the two iterators here (especially if they are known to be sorted)
	//
	// But for now, this is a proof-of-concept, so the first one, one-by-one (no batching).
	// This is going to be the crux of a lot of statistics optimizations. Many others are

	return func(yield func(Relation, error) bool) {
		for _, rid := range resourceIds {
			subit, err := a.left.LookupSubjects(ctx, rid)
			if err != nil {
				yield(Relation{}, err)
				return
			}
			for rel, err := range subit {
				if err != nil {
					yield(Relation{}, err)
					return
				}
				checkit, err := a.right.Check(ctx, []string{rel.Subject.ObjectID}, subjectId)
				if err != nil {
					yield(Relation{}, err)
					return
				}
				for checkrel, err := range checkit {
					if err != nil {
						yield(Relation{}, err)
						return
					}
					combinedrel := Relation{
						OptionalCaveat:     checkrel.OptionalCaveat,
						OptionalExpiration: checkrel.OptionalExpiration,
						OptionalIntegrity:  checkrel.OptionalIntegrity,
						RelationshipReference: tuple.RelationshipReference{
							Resource: rel.RelationshipReference.Resource,
							Subject:  checkrel.RelationshipReference.Subject,
						},
					}
					if !yield(combinedrel, nil) {
						return
					}
				}
			}
		}
	}, nil
}

func (a *Arrow) LookupSubjects(ctx *Context, resourceId string) (RelationSeq, error) {
	panic("not implemented")
}

func (a *Arrow) LookupResources(ctx *Context, subjectId string) (RelationSeq, error) {
	panic("not implemented")
}

func (a *Arrow) Clone() Iterator {
	return &Arrow{
		left:  a.left.Clone(),
		right: a.right.Clone(),
	}
}

func (a *Arrow) Explain() Explain {
	return Explain{
		Info:       "Arrow",
		SubExplain: []Explain{a.left.Explain(), a.right.Explain()},
	}
}
