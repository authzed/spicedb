package query

import (
	"fmt"
	"io"
	"sort"
	"time"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	// NullIteratorType and FixedIteratorType both produce a FixedIterator;
	// the Null form is just the empty-paths case (see Decompile).
	MustRegisterIterator(IteratorSpec{
		Type: NullIteratorType,
		Name: "Null",
		ConstructWithArgs: func(_ *IteratorArgs, _ []Iterator, key CanonicalKey) (Iterator, error) {
			fixed := NewFixedIterator()
			fixed.canonicalKey = key
			return fixed, nil
		},
		Deserialize: deserializeNull,
	})
	MustRegisterIterator(IteratorSpec{
		Type: FixedIteratorType,
		Name: "Fixed",
		ConstructWithArgs: func(args *IteratorArgs, _ []Iterator, key CanonicalKey) (Iterator, error) {
			var fixed *FixedIterator
			if args != nil {
				fixed = NewFixedIterator(args.FixedPaths...)
			} else {
				fixed = NewFixedIterator()
			}
			fixed.canonicalKey = key
			return fixed, nil
		},
		Deserialize: deserializeFixed,
	})
}

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

const fixedFlagHasPaths = 0

func (f *FixedIterator) Serialize(w io.Writer) error {
	// Empty FixedIterator decompiles to NullIteratorType; emit Null on the wire
	// so the receiver reconstructs the same shape.
	if len(f.paths) == 0 {
		return SerializeWithHeader(w, NullIteratorType, f.canonicalKey, func(buf io.Writer) error {
			return writeUvarint(buf, 0)
		})
	}
	return SerializeWithHeader(w, FixedIteratorType, f.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, fixedFlagHasPaths, len(f.paths) > 0)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if !hasFlag(flags, fixedFlagHasPaths) {
			return nil
		}
		if err := writeUvarint(buf, uint64(len(f.paths))); err != nil {
			return err
		}
		for i := range f.paths {
			if err := serializePath(buf, &f.paths[i]); err != nil {
				return fmt.Errorf("path %d: %w", i, err)
			}
		}
		return nil
	})
}

func deserializeFixed(body io.Reader, key CanonicalKey, _ *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("fixed flags: %w", err)
	}
	var paths []Path
	if hasFlag(flags, fixedFlagHasPaths) {
		n, err := readUvarint(br)
		if err != nil {
			return nil, fmt.Errorf("fixed path count: %w", err)
		}
		if n > maxSubCount {
			return nil, fmt.Errorf("fixed path count %d exceeds %d", n, maxSubCount)
		}
		paths = make([]Path, n)
		for i := range paths {
			p, err := deserializePath(br)
			if err != nil {
				return nil, fmt.Errorf("fixed path %d: %w", i, err)
			}
			paths[i] = p
		}
	}
	f := NewFixedIterator(paths...)
	f.canonicalKey = key
	return f, nil
}

// deserializeNull reconstructs the empty FixedIterator emitted by Fixed.Serialize
// when len(paths) == 0. Null shares the FixedIterator type but uses a distinct
// type byte so old plans that distinguished the two stay round-trippable.
func deserializeNull(body io.Reader, key CanonicalKey, _ *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		return nil, fmt.Errorf("null body: %w", err)
	}
	f := NewFixedIterator()
	f.canonicalKey = key
	return f, nil
}

// Path serialization (used only by FixedIterator).
//
// Path.Metadata is runtime-only (map[string]any with arbitrary executor-
// produced values) and is intentionally not serialized — round-trip equality
// tests must ignore it.

const (
	pathFlagCaveat = iota
	pathFlagExpiration
	pathFlagExcluded
	pathFlagIntegrity
)

func serializePath(w io.Writer, p *Path) error {
	var flags uint64
	setFlag(&flags, pathFlagCaveat, p.Caveat != nil)
	setFlag(&flags, pathFlagExpiration, p.Expiration != nil)
	setFlag(&flags, pathFlagExcluded, len(p.ExcludedSubjects) > 0)
	setFlag(&flags, pathFlagIntegrity, len(p.Integrity) > 0)
	if err := writeUvarint(w, flags); err != nil {
		return err
	}
	if err := writeString(w, p.Resource.ObjectType); err != nil {
		return err
	}
	if err := writeString(w, p.Resource.ObjectID); err != nil {
		return err
	}
	if err := writeString(w, p.Relation); err != nil {
		return err
	}
	if err := writeString(w, p.Subject.ObjectType); err != nil {
		return err
	}
	if err := writeString(w, p.Subject.ObjectID); err != nil {
		return err
	}
	if err := writeString(w, p.Subject.Relation); err != nil {
		return err
	}
	if hasFlag(flags, pathFlagCaveat) {
		if err := writeProto(w, p.Caveat); err != nil {
			return err
		}
	}
	if hasFlag(flags, pathFlagExpiration) {
		if err := writeUvarint(w, uint64(p.Expiration.UnixNano())); err != nil {
			return err
		}
	}
	if hasFlag(flags, pathFlagExcluded) {
		if err := writeUvarint(w, uint64(len(p.ExcludedSubjects))); err != nil {
			return err
		}
		for i, sub := range p.ExcludedSubjects {
			if err := serializePath(w, sub); err != nil {
				return fmt.Errorf("excluded[%d]: %w", i, err)
			}
		}
	}
	if hasFlag(flags, pathFlagIntegrity) {
		if err := writeUvarint(w, uint64(len(p.Integrity))); err != nil {
			return err
		}
		for i, integ := range p.Integrity {
			if err := writeProto(w, integ); err != nil {
				return fmt.Errorf("integrity[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func deserializePath(r byteReader) (Path, error) {
	flags, err := readUvarint(r)
	if err != nil {
		return Path{}, err
	}
	resType, err := readString(r)
	if err != nil {
		return Path{}, err
	}
	resID, err := readString(r)
	if err != nil {
		return Path{}, err
	}
	rel, err := readString(r)
	if err != nil {
		return Path{}, err
	}
	subType, err := readString(r)
	if err != nil {
		return Path{}, err
	}
	subID, err := readString(r)
	if err != nil {
		return Path{}, err
	}
	subRel, err := readString(r)
	if err != nil {
		return Path{}, err
	}
	p := Path{
		Resource: Object{ObjectType: resType, ObjectID: resID},
		Relation: rel,
		Subject:  ObjectAndRelation{ObjectType: subType, ObjectID: subID, Relation: subRel},
		Metadata: make(map[string]any),
	}
	if hasFlag(flags, pathFlagCaveat) {
		cav := &core.CaveatExpression{}
		if err := readProto(r, cav); err != nil {
			return Path{}, fmt.Errorf("caveat: %w", err)
		}
		p.Caveat = cav
	}
	if hasFlag(flags, pathFlagExpiration) {
		ns, err := readUvarint(r)
		if err != nil {
			return Path{}, fmt.Errorf("expiration: %w", err)
		}
		//nolint:gosec  // this is intentional reinterpretation
		t := time.Unix(0, int64(ns))
		p.Expiration = &t
	}
	if hasFlag(flags, pathFlagExcluded) {
		n, err := readUvarint(r)
		if err != nil {
			return Path{}, fmt.Errorf("excluded count: %w", err)
		}
		if n > maxSubCount {
			return Path{}, fmt.Errorf("excluded count %d exceeds %d", n, maxSubCount)
		}
		p.ExcludedSubjects = make([]*Path, n)
		for i := range p.ExcludedSubjects {
			sub, err := deserializePath(r)
			if err != nil {
				return Path{}, fmt.Errorf("excluded[%d]: %w", i, err)
			}
			p.ExcludedSubjects[i] = &sub
		}
	}
	if hasFlag(flags, pathFlagIntegrity) {
		n, err := readUvarint(r)
		if err != nil {
			return Path{}, fmt.Errorf("integrity count: %w", err)
		}
		if n > maxSubCount {
			return Path{}, fmt.Errorf("integrity count %d exceeds %d", n, maxSubCount)
		}
		p.Integrity = make([]*core.RelationshipIntegrity, n)
		for i := range p.Integrity {
			ri := &core.RelationshipIntegrity{}
			if err := readProto(r, ri); err != nil {
				return Path{}, fmt.Errorf("integrity[%d]: %w", i, err)
			}
			p.Integrity[i] = ri
		}
	}
	return p, nil
}
