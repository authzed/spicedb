package query

import (
	"fmt"
	"io"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/query/planhint"
)

func init() {
	MustRegisterIterator(IteratorSpec{
		Type: UnionIteratorType,
		Name: "Union",
		ConstructWithArgs: func(_ *IteratorArgs, subs []Iterator, key CanonicalKey) (Iterator, error) {
			it := NewUnionIterator(subs...)
			// NewUnionIterator returns a FixedIterator when subs is empty.
			switch v := it.(type) {
			case *UnionIterator:
				v.canonicalKey = key
			case *FixedIterator:
				v.canonicalKey = key
			}
			return it, nil
		},
		Deserialize: deserializeUnion,
	})
}

// UnionIterator the set of paths that are in any of underlying subiterators.
// This is equivalent to `permission foo = bar | baz`
type UnionIterator struct {
	subIts                   []Iterator
	canonicalKey             CanonicalKey
	checkConcurrency         uint16
	concurrencyHint          planhint.ID
	independentCheckBranches bool
}

var _ Iterator = &UnionIterator{}

func NewUnionIterator(subiterators ...Iterator) Iterator {
	if len(subiterators) == 0 {
		return NewFixedIterator() // Return empty FixedIterator instead of empty Union
	}
	return &UnionIterator{
		subIts: subiterators,
	}
}

func (u *UnionIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// Native groups schema-owned target types only after direct membership has
	// had the opportunity to grant. Preserve that phase boundary rather than
	// parallelizing the entire relation and speculating on its direct forms.
	if ctx.checkExecution.BatchChecks && u.checkConcurrency > 1 && sourceGroupCandidate(u.subIts) {
		batch := resourceBatch([]Object{resource}, subject)
		batch.mode = AllowSingleResult

		if paths, handled, err := checkSourceGroups(ctx, u, batch); handled {
			if err != nil {
				return nil, err
			}

			return paths[0], nil
		}
	}

	// Read the concurrency recommendation only for independent permission operands.
	// Relation forms retain direct-before-indirect order so a direct grant can
	// avoid indirect userset work. Observers/frontiers retain serial ownership too.
	limit, err := ctx.independentCheckConcurrency(u.checkConcurrency, u.concurrencyHint,
		u.independentCheckBranches || namedBatchBranches(u.subIts))
	if err != nil {
		return nil, err
	}

	if limit > 1 {
		return checkSetParallel(ctx, u.subIts, resource, subject, limit, setCheckUnion)
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(u, "processing %d sub-iterators for resource %s:%s", len(u.subIts), resource.ObjectType, resource.ObjectID)
	}

	// Visit all sub-iterators and OR-merge any matching paths.
	var result *Path
	for iterIdx, it := range u.subIts {
		if ctx.shouldTrace() {
			ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)
		}

		path, err := ctx.Check(it, resource, subject)
		if err != nil {
			return nil, err
		}

		if path == nil {
			if ctx.shouldTrace() {
				ctx.TraceStep(u, "sub-iterator %d: no match", iterIdx)
			}
			continue
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(u, "sub-iterator %d: matched", iterIdx)
		}

		result, err = result.MergeOr(path)
		if err != nil {
			return nil, err
		}

		if result.Caveat == nil {
			if ctx.shouldTrace() {
				ctx.TraceStep(u, "sub-iterator %d matched uncaveated, short-circuiting", iterIdx)
			}
			return result, nil
		}
	}

	return result, nil
}

func (u *UnionIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(u, "processing %d sub-iterators for resource %s:%s", len(u.subIts), resource.ObjectType, resource.ObjectID)
	}

	// Create a concatenated sequence from all sub-iterators
	combinedSeq := func(yield func(*Path, error) bool) {
		for iterIdx, it := range u.subIts {
			if ctx.shouldTrace() {
				ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)
			}

			pathSeq, err := ctx.IterSubjects(it, resource, filterSubjectType)
			if err != nil {
				yield(nil, err)
				return
			}

			pathCount := 0
			for path, err := range pathSeq {
				if err != nil {
					yield(nil, err)
					return
				}
				pathCount++
				if !yield(path, nil) {
					return
				}
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(u, "sub-iterator %d returned %d paths", iterIdx, pathCount)
			}
		}
	}

	// Wrap with deduplication
	return DeduplicatePathSeq(combinedSeq), nil
}

func (u *UnionIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(
			u,
			"processing %d sub-iterators for subject %s:%s#%s",
			len(u.subIts),
			subject.ObjectType,
			subject.ObjectID,
			subject.Relation,
		)
	}

	// Create a concatenated sequence from all sub-iterators
	combinedSeq := func(yield func(*Path, error) bool) {
		for iterIdx, it := range u.subIts {
			if ctx.shouldTrace() {
				ctx.TraceStep(u, "processing sub-iterator %d", iterIdx)
			}

			pathSeq, err := ctx.IterResources(it, subject, filterResourceType)
			if err != nil {
				yield(nil, err)
				return
			}

			pathCount := 0
			for path, err := range pathSeq {
				if err != nil {
					yield(nil, err)
					return
				}
				pathCount++
				if !yield(path, nil) {
					return
				}
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(u, "sub-iterator %d returned %d paths", iterIdx, pathCount)
			}
		}
	}

	// Wrap with deduplication
	return DeduplicatePathSeq(combinedSeq), nil
}

func (u *UnionIterator) Clone() Iterator {
	cloned := &UnionIterator{
		canonicalKey:             u.canonicalKey,
		independentCheckBranches: u.independentCheckBranches,
		checkConcurrency:         u.checkConcurrency,
		concurrencyHint:          u.concurrencyHint,
		subIts:                   make([]Iterator, len(u.subIts)),
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
	return &UnionIterator{
		canonicalKey:             u.canonicalKey,
		independentCheckBranches: u.independentCheckBranches,
		checkConcurrency:         u.checkConcurrency,
		concurrencyHint:          u.concurrencyHint,
		subIts:                   newSubs,
	}, nil
}

func (u *UnionIterator) CanonicalKey() CanonicalKey {
	return u.canonicalKey
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

func (u *UnionIterator) Serialize(w io.Writer) error {
	return SerializeWithHeader(w, UnionIteratorType, u.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, independentCheckBranchesFlag, u.independentCheckBranches)

		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		return writeSubs(buf, u.subIts)
	})
}

func deserializeUnion(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("union flags: %w", err)
	}
	subs, err := readSubs(br, dctx)
	if err != nil {
		return nil, err
	}
	it := NewUnionIterator(subs...)
	switch v := it.(type) {
	case *UnionIterator:
		v.canonicalKey = key
		v.independentCheckBranches = hasFlag(flags, independentCheckBranchesFlag)
	case *FixedIterator:
		v.canonicalKey = key
	}
	return it, nil
}
