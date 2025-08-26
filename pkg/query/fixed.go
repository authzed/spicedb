package query

import "fmt"

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

func (f *FixedIterator) LookupSubjects(ctx *Context, resourceID string) (RelationSeq, error) {
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

func (f *FixedIterator) LookupResources(ctx *Context, subjectID string) (RelationSeq, error) {
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
