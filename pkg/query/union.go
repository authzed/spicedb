package query

import (
	"github.com/google/uuid"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

// UnionIterator the set of paths that are in any of underlying subiterators.
// This is equivalent to `permission foo = bar | baz`
type UnionIterator struct {
	id           string
	subIts       []Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &UnionIterator{}

func NewUnionIterator(subiterators ...Iterator) Iterator {
	if len(subiterators) == 0 {
		return NewFixedIterator() // Return empty FixedIterator instead of empty Union
	}
	return &UnionIterator{
		id:     uuid.NewString(),
		subIts: subiterators,
	}
}

func (u *UnionIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	ctx.TraceStep(u, "processing %d sub-iterators with %d resources", len(u.subIts), len(resources))

	// Create a concatenated sequence from all sub-iterators
	combinedSeq := func(yield func(Path, error) bool) {
		for iterIdx, it := range u.subIts {
			ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)

			pathSeq, err := ctx.Check(it, resources, subject)
			if err != nil {
				yield(Path{}, err)
				return
			}

			pathCount := 0
			for path, err := range pathSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				pathCount++
				if !yield(path, nil) {
					return
				}
			}

			ctx.TraceStep(u, "sub-iterator %d returned %d paths", iterIdx, pathCount)
		}
	}

	// Wrap with deduplication
	return DeduplicatePathSeq(combinedSeq), nil
}

func (u *UnionIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	ctx.TraceStep(u, "processing %d sub-iterators for resource %s:%s", len(u.subIts), resource.ObjectType, resource.ObjectID)

	// Create a concatenated sequence from all sub-iterators
	combinedSeq := func(yield func(Path, error) bool) {
		for iterIdx, it := range u.subIts {
			ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)

			pathSeq, err := ctx.IterSubjects(it, resource, filterSubjectType)
			if err != nil {
				yield(Path{}, err)
				return
			}

			pathCount := 0
			for path, err := range pathSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				pathCount++
				if !yield(path, nil) {
					return
				}
			}

			ctx.TraceStep(u, "sub-iterator %d returned %d paths", iterIdx, pathCount)
		}
	}

	// Wrap with deduplication
	return DeduplicatePathSeq(combinedSeq), nil
}

func (u *UnionIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	ctx.TraceStep(u, "processing %d sub-iterators for subject %s:%s#%s", len(u.subIts), subject.ObjectType, subject.ObjectID, subject.Relation)

	// Create a concatenated sequence from all sub-iterators
	combinedSeq := func(yield func(Path, error) bool) {
		for iterIdx, it := range u.subIts {
			ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)

			pathSeq, err := ctx.IterResources(it, subject, filterResourceType)
			if err != nil {
				yield(Path{}, err)
				return
			}

			pathCount := 0
			for path, err := range pathSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				pathCount++
				if !yield(path, nil) {
					return
				}
			}

			ctx.TraceStep(u, "sub-iterator %d returned %d paths", iterIdx, pathCount)
		}
	}

	// Wrap with deduplication
	return DeduplicatePathSeq(combinedSeq), nil
}

func (u *UnionIterator) Clone() Iterator {
	cloned := &UnionIterator{
		id:     uuid.NewString(),
		subIts: make([]Iterator, len(u.subIts)),
	}
	for idx, subIt := range u.subIts {
		cloned.subIts[idx] = subIt.Clone()
	}
	return cloned
}

func (u *UnionIterator) Explain() Explain {
	subs := make([]Explain, len(u.subIts))
	for i, it := range u.subIts {
		subs[i] = it.Explain()
	}
	return Explain{
		Name:       "Union",
		Info:       "Union",
		SubExplain: subs,
	}
}

func (u *UnionIterator) Subiterators() []Iterator {
	return u.subIts
}

func (u *UnionIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &UnionIterator{id: uuid.NewString(), subIts: newSubs}, nil
}

func (u *UnionIterator) ID() string {
	return u.id
}

func (u *UnionIterator) ResourceType() ([]ObjectType, error) {
	if len(u.subIts) == 0 {
		return []ObjectType{}, nil
	}

	// Collect all resource types from sub-iterators and deduplicate
	result := mapz.NewSet[ObjectType]()

	for _, subIt := range u.subIts {
		subTypes, err := subIt.ResourceType()
		if err != nil {
			return nil, err
		}
		result.Extend(subTypes)
	}

	return result.AsSlice(), nil
}

func (u *UnionIterator) SubjectTypes() ([]ObjectType, error) {
	return collectAndDeduplicateSubjectTypes(u.subIts)
}
