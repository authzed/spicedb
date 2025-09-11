package query

import (
	"github.com/authzed/spicedb/pkg/tuple"
)

// Alias is an iterator that rewrites the Resource's Relation field of all relations
// streamed from the sub-iterator to a specified alias relation.
type Alias struct {
	relation string
	subIt    Iterator
}

var _ Iterator = &Alias{}

// NewAlias creates a new Alias iterator that rewrites relations from the sub-iterator
// to use the specified relation name.
func NewAlias(relation string, subIt Iterator) *Alias {
	return &Alias{
		relation: relation,
		subIt:    subIt,
	}
}

func (a *Alias) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	// First, check for self-edge: if the object with internal relation matches the subject
	for _, resource := range resources {
		resourceWithAlias := resource.WithRelation(a.relation)
		if resourceWithAlias.ObjectID == subject.ObjectID &&
			resourceWithAlias.ObjectType == subject.ObjectType &&
			resourceWithAlias.Relation == subject.Relation {
			// Return the self-edge relation first
			selfRelation := Relation{
				RelationshipReference: tuple.RelationshipReference{
					Resource: resourceWithAlias,
					Subject:  subject,
				},
			}

			// Also get relations from sub-iterator
			subSeq, err := a.subIt.CheckImpl(ctx, resources, subject)
			if err != nil {
				return nil, err
			}

			return func(yield func(Relation, error) bool) {
				// Yield the self-edge first
				if !yield(selfRelation, nil) {
					return
				}

				// Then yield rewritten relations from sub-iterator
				for rel, err := range subSeq {
					if err != nil {
						yield(rel, err)
						return
					}

					rewrittenRel := a.rewriteRelation(rel)
					if !yield(rewrittenRel, nil) {
						return
					}
				}
			}, nil
		}
	}

	// No self-edge detected, just rewrite relations from sub-iterator
	subSeq, err := a.subIt.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(Relation, error) bool) {
		for rel, err := range subSeq {
			if err != nil {
				yield(rel, err)
				return
			}

			rewrittenRel := a.rewriteRelation(rel)
			if !yield(rewrittenRel, nil) {
				return
			}
		}
	}, nil
}

func (a *Alias) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	subSeq, err := a.subIt.IterSubjectsImpl(ctx, resource)
	if err != nil {
		return nil, err
	}

	return func(yield func(Relation, error) bool) {
		for rel, err := range subSeq {
			if err != nil {
				yield(rel, err)
				return
			}

			rewrittenRel := a.rewriteRelation(rel)
			if !yield(rewrittenRel, nil) {
				return
			}
		}
	}, nil
}

func (a *Alias) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	subSeq, err := a.subIt.IterResourcesImpl(ctx, subject)
	if err != nil {
		return nil, err
	}

	return func(yield func(Relation, error) bool) {
		for rel, err := range subSeq {
			if err != nil {
				yield(rel, err)
				return
			}

			rewrittenRel := a.rewriteRelation(rel)
			if !yield(rewrittenRel, nil) {
				return
			}
		}
	}, nil
}

// rewriteRelation rewrites the Resource's Relation field to the alias relation
func (a *Alias) rewriteRelation(rel Relation) Relation {
	rewritten := rel
	rewritten.Resource.Relation = a.relation
	return rewritten
}

func (a *Alias) Clone() Iterator {
	return &Alias{
		relation: a.relation,
		subIt:    a.subIt.Clone(),
	}
}

func (a *Alias) Explain() Explain {
	return Explain{
		Info:       "Alias(" + a.relation + ")",
		SubExplain: []Explain{a.subIt.Explain()},
	}
}
