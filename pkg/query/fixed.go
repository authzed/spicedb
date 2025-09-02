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

func (f *FixedIterator) Check(ctx *Context, resourceIDs []string, subjectID string) (RelationSeq, error) {
	return func(yield func(Relation, error) bool) {
		for _, rel := range f.rels {
			for _, resourceID := range resourceIDs {
				if rel.Resource.ObjectID == resourceID {
					if rel.Subject.ObjectID == subjectID {
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

func (f *FixedIterator) IterSubjects(ctx *Context, resourceID string) (RelationSeq, error) {
	return func(yield func(Relation, error) bool) {
		for _, rel := range f.rels {
			// Check if the relation's resource matches the requested resource ID
			if rel.Resource.ObjectID == resourceID {
				if !yield(rel, nil) {
					return
				}
			}
		}
	}, nil
}

func (f *FixedIterator) IterResources(ctx *Context, subjectID string) (RelationSeq, error) {
	return func(yield func(Relation, error) bool) {
		for _, rel := range f.rels {
			// Check if the relation's subject matches the requested subject ID
			if rel.Subject.ObjectID == subjectID {
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
