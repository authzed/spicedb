package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// FixedIterator represents a fixed set of pre-computed paths.
// This is often useful for testing, but can also be used in rare situations
// where we'd like to force a set of intermediate paths.

// For example: document->folder->ownerGroup->user -- and we'd like to
// find all documents (IterResources) that traverse a known folder->ownerGroup relationship
type FixedIterator struct {
	paths []Path
}

var _ Iterator = &FixedIterator{}

func NewFixedIterator(paths ...Path) *FixedIterator {
	return &FixedIterator{
		paths: paths,
	}
}

func (f *FixedIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		var resultPaths []Path
		ctx.TraceStep(f, "checking %d paths against %d resources", len(f.paths), len(resources))

		for _, path := range f.paths {
			for _, resource := range resources {
				if path.Resource.Equals(resource) &&
					GetObject(path.Subject).Equals(GetObject(subject)) {
					resultPaths = append(resultPaths, path)
					if !yield(path, nil) {
						return
					}
					break
				}
			}
		}

		ctx.TraceStep(f, "found %d matching paths", len(resultPaths))
	}, nil
}

func (f *FixedIterator) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		var resultPaths []Path

		ctx.TraceStep(f, "iterating subjects for resource %s:%s from %d paths", resource.ObjectType, resource.ObjectID, len(f.paths))

		for _, path := range f.paths {
			// Check if the path's resource matches the requested resource
			if path.Resource.Equals(resource) {
				resultPaths = append(resultPaths, path)
				if !yield(path, nil) {
					return
				}
			}
		}

		ctx.TraceStep(f, "found %d matching subjects", len(resultPaths))
	}, nil
}

func (f *FixedIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		var resultPaths []Path

		ctx.TraceStep(f, "iterating resources for subject %s:%s from %d paths", subject.ObjectType, subject.ObjectID, len(f.paths))

		for _, path := range f.paths {
			// Check if the path's subject matches the requested subject
			if path.Subject.ObjectID == subject.ObjectID && path.Subject.ObjectType == subject.ObjectType && path.Subject.Relation == subject.Relation {
				resultPaths = append(resultPaths, path)
				if !yield(path, nil) {
					return
				}
			}
		}

		ctx.TraceStep(f, "found %d matching resources", len(resultPaths))
	}, nil
}

func (f *FixedIterator) Explain() Explain {
	return Explain{
		Name: "Fixed",
		Info: fmt.Sprintf("Fixed(%d paths)", len(f.paths)),
	}
}

func (f *FixedIterator) Clone() Iterator {
	// Create a copy of the paths slice
	clonedPaths := make([]Path, len(f.paths))
	copy(clonedPaths, f.paths)

	return &FixedIterator{
		paths: clonedPaths,
	}
}

func (f *FixedIterator) Subiterators() []Iterator {
	return nil
}

func (f *FixedIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a leaf FixedIterator's subiterators")
}
