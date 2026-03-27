package query

import (
	"fmt"
	"sort"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// sortObjectTypes sorts a slice of ObjectType for deterministic ordering.
// This prevents test flakiness from nondeterministic map iteration.
func sortObjectTypes(types []ObjectType) {
	sort.Slice(types, func(i, j int) bool {
		if types[i].Type != types[j].Type {
			return types[i].Type < types[j].Type
		}
		return types[i].Subrelation < types[j].Subrelation
	})
}

// FixedIterator represents a fixed set of pre-computed paths.
// This is often useful for testing, but can also be used in rare situations
// where we'd like to force a set of intermediate paths.

// For example: document->folder->ownerGroup->user -- and we'd like to
// find all documents (IterResources) that traverse a known folder->ownerGroup relationship
type FixedIterator struct {
	paths        []Path
	resourceType ObjectType
	subjectTypes []ObjectType
	canonicalKey CanonicalKey
}

var _ Iterator = &FixedIterator{}

func NewFixedIterator(paths ...Path) *FixedIterator {
	var resourceType ObjectType
	subjectTypeMap := make(map[string]ObjectType)

	if len(paths) > 0 {
		// Set resource type from first path - just the type, not the relation
		// Note: For simplicity, we use the resource type from the first path.
		// Ideally, all paths should have the same resource type, but this isn't strictly enforced.
		resourceType = ObjectType{
			Type:        paths[0].Resource.ObjectType,
			Subrelation: tuple.Ellipsis, // Resource types use ellipsis
		}

		// Collect subject types from all paths
		for _, path := range paths {
			subjectType := ObjectType{
				Type:        path.Subject.ObjectType,
				Subrelation: path.Subject.Relation,
			}
			key := subjectType.Type + "#" + subjectType.Subrelation
			subjectTypeMap[key] = subjectType
		}
	}

	// Convert subject types map to slice
	subjectTypes := make([]ObjectType, 0, len(subjectTypeMap))
	for _, st := range subjectTypeMap {
		subjectTypes = append(subjectTypes, st)
	}

	// Sort to ensure deterministic order (prevent test flakiness from map iteration)
	sortObjectTypes(subjectTypes)

	return &FixedIterator{
		paths:        paths,
		resourceType: resourceType,
		subjectTypes: subjectTypes,
	}
}

func (f *FixedIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(f, "checking %d paths against resource %s:%s", len(f.paths), resource.ObjectType, resource.ObjectID)
	}

	for _, path := range f.paths {
		if path.Resource.Equals(resource) &&
			GetObject(path.Subject).Equals(GetObject(subject)) {
			if ctx.shouldTrace() {
				ctx.TraceStep(f, "found matching path")
			}
			return &path, nil
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(f, "no matching path found")
	}
	return nil, nil
}

func (f *FixedIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		count := 0
		if ctx.shouldTrace() {
			ctx.TraceStep(f, "iterating subjects for resource %s:%s from %d paths", resource.ObjectType, resource.ObjectID, len(f.paths))
		}

		for i := range f.paths {
			// Check if the path's resource matches the requested resource
			if f.paths[i].Resource.Equals(resource) {
				count++
				if !yield(&f.paths[i], nil) {
					return
				}
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(f, "found %d matching subjects", count)
		}
	}, nil
}

func (f *FixedIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		count := 0
		if ctx.shouldTrace() {
			ctx.TraceStep(f, "iterating resources for subject %s:%s from %d paths", subject.ObjectType, subject.ObjectID, len(f.paths))
		}

		for i := range f.paths {
			// Check if the path's subject matches the requested subject
			if f.paths[i].Subject.ObjectID == subject.ObjectID && f.paths[i].Subject.ObjectType == subject.ObjectType && f.paths[i].Subject.Relation == subject.Relation {
				count++
				if !yield(&f.paths[i], nil) {
					return
				}
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(f, "found %d matching resources", count)
		}
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

	// Create a copy of subject types slice
	clonedSubjectTypes := make([]ObjectType, len(f.subjectTypes))
	copy(clonedSubjectTypes, f.subjectTypes)

	return &FixedIterator{
		canonicalKey: f.canonicalKey,
		paths:        clonedPaths,
		resourceType: f.resourceType,
		subjectTypes: clonedSubjectTypes,
	}
}

func (f *FixedIterator) Subiterators() []Iterator {
	return nil
}

func (f *FixedIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a leaf FixedIterator's subiterators")
}

func (f *FixedIterator) CanonicalKey() CanonicalKey {
	return f.canonicalKey
}

func (f *FixedIterator) ResourceType() ([]ObjectType, error) {
	if f.resourceType.Type == "" {
		return []ObjectType{}, nil
	}
	return []ObjectType{f.resourceType}, nil
}

func (f *FixedIterator) SubjectTypes() ([]ObjectType, error) {
	return f.subjectTypes, nil
}
