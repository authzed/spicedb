package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Arrow is an iterator that represents the set of relations that
// follow from a walk in the graph.
//
// Ex: `folder->owner` and `left->right`
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

func (a *Arrow) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	// TODO -- the ordering, directionality, batching, everything can depend on other statistics.
	//
	// There are three major strategies:
	// - IterSubjects on the left, Check on the right (as per this implementation)
	// - IterResources on the right, Check on the left
	// - IterSubjects on left, IterResources on right, and intersect the two iterators here (especially if they are known to be sorted)
	//
	// But for now, this is a proof-of-concept, so the first one, one-by-one (no batching).
	// This is going to be the crux of a lot of statistics optimizations -- statistics often
	// don't restructure the tree, but can affect the best way to evaluate the tree, sometimes dynamically.

	return func(yield func(Relation, error) bool) {
		for _, resource := range resources {
			subit, err := a.left.IterSubjectsImpl(ctx, resource)
			if err != nil {
				yield(Relation{}, err)
				return
			}
			for rel, err := range subit {
				if err != nil {
					yield(Relation{}, err)
					return
				}
				checkResources := []Object{{ObjectID: rel.Subject.ObjectID, ObjectType: rel.Subject.ObjectType}}
				checkit, err := a.right.CheckImpl(ctx, checkResources, subject)
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
							Resource: rel.Resource,
							Subject:  checkrel.Subject,
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

func (a *Arrow) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (a *Arrow) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
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
