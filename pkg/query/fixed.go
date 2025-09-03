package query

import "fmt"

// FixedIterator represents a fixed set of pre-computed relations.
// This is often useful for testing, but can also be used in rare situations
// where we'd like to force a set of intermediate relations.

// For example: document->folder->ownerGroup->user -- and we'd like to
// find all documents (IterResources) that traverse a known folder->ownerGroup relationship
type FixedIterator struct {
	rels []Relation
}

var _ Iterator = &FixedIterator{}

func NewFixedIterator(rels ...Relation) *FixedIterator {
	return &FixedIterator{
		rels: rels,
	}
}

func (f *FixedIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	return func(yield func(Relation, error) bool) {
		for _, rel := range f.rels {
			for _, resource := range resources {
				if rel.Resource.ObjectID == resource.ObjectID && rel.Resource.ObjectType == resource.ObjectType {
					if rel.Subject.ObjectID == subject.ObjectID && rel.Subject.ObjectType == subject.ObjectType && rel.Subject.Relation == subject.Relation {
						ok := yield(rel, nil)
						if !ok {
							return
						}
					}
					break
				}
			}
		}
	}, nil
}

func (f *FixedIterator) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return func(yield func(Relation, error) bool) {
		for _, rel := range f.rels {
			// Check if the relation's resource matches the requested resource
			if rel.Resource.ObjectID == resource.ObjectID && rel.Resource.ObjectType == resource.ObjectType {
				if !yield(rel, nil) {
					return
				}
			}
		}
	}, nil
}

func (f *FixedIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	return func(yield func(Relation, error) bool) {
		for _, rel := range f.rels {
			// Check if the relation's subject matches the requested subject
			if rel.Subject.ObjectID == subject.ObjectID && rel.Subject.ObjectType == subject.ObjectType && rel.Subject.Relation == subject.Relation {
				if !yield(rel, nil) {
					return
				}
			}
		}
	}, nil
}

func (f *FixedIterator) Explain() Explain {
	return Explain{
		Info: fmt.Sprintf("Fixed(%d relations)", len(f.rels)),
	}
}

func (f *FixedIterator) Clone() Iterator {
	// Create a copy of the relations slice
	clonedRels := make([]Relation, len(f.rels))
	copy(clonedRels, f.rels)

	return &FixedIterator{
		rels: clonedRels,
	}
}
