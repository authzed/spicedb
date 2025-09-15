package query

import "fmt"

// FixedIterator represents a fixed set of pre-computed paths.
// This is often useful for testing, but can also be used in rare situations
// where we'd like to force a set of intermediate paths.

// For example: document->folder->ownerGroup->user -- and we'd like to
// find all documents (IterResources) that traverse a known folder->ownerGroup relationship
type FixedIterator struct {
	paths []*Path
}

var _ Iterator = &FixedIterator{}

func NewFixedIterator(paths ...*Path) *FixedIterator {
	return &FixedIterator{
		paths: paths,
	}
}

func (f *FixedIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		for _, path := range f.paths {
			for _, resource := range resources {
				if path.Resource.Equals(resource) &&
					GetObject(path.Subject).Equals(GetObject(subject)) {
					if !yield(path, nil) {
						return
					}
					break
				}
			}
		}
	}, nil
}

func (f *FixedIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		for _, path := range f.paths {
			// Check if the path's resource matches the requested resource
			if path.Resource.Equals(resource) {
				if !yield(path, nil) {
					return
				}
			}
		}
	}, nil
}

func (f *FixedIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		for _, path := range f.paths {
			// Check if the path's subject matches the requested subject
			if path.Subject.ObjectID == subject.ObjectID && path.Subject.ObjectType == subject.ObjectType && path.Subject.Relation == subject.Relation {
				if !yield(path, nil) {
					return
				}
			}
		}
	}, nil
}

func (f *FixedIterator) Explain() Explain {
	return Explain{
		Info: fmt.Sprintf("Fixed(%d paths)", len(f.paths)),
	}
}

func (f *FixedIterator) Clone() Iterator {
	// Create a copy of the paths slice
	clonedPaths := make([]*Path, len(f.paths))
	copy(clonedPaths, f.paths)

	return &FixedIterator{
		paths: clonedPaths,
	}
}
