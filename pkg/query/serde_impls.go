package query

import (
	"errors"
	"fmt"
	"io"
	"time"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// This file holds the per-iterator Serialize methods and Deserialize functions
// that are wired into each IteratorSpec from the iterator file's init().
// Centralizing them here lets the wire format evolve in one place; each
// iterator file just references the matching deserialize func by name.
//
// Wire format (also documented in serde.go):
//   [type byte][uvarint keyLen][key][uvarint bodyLen][flags uvarint][body...]
//
// Each iterator's body begins with a uvarint flags bitset. Empty strings, false
// bools, nil pointers, and zero-length slices are signaled by their bit being
// clear and emit nothing — new fields append at higher bit positions for
// forward compatibility.

// ----- Null (empty FixedIterator) -----

// Null shares the FixedIterator type. It's emitted only when Decompile sees an
// empty FixedIterator, so the wire byte alone is enough to reconstruct it.
// The fixedIterator's own Serialize emits NullIteratorType when paths are empty.
func deserializeNull(body io.Reader, key CanonicalKey, _ *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		// Empty flags expected, but we tolerate any trailing bytes for forward compat.
		return nil, fmt.Errorf("null body: %w", err)
	}
	f := NewFixedIterator()
	f.canonicalKey = key
	return f, nil
}

// ----- Datastore -----

const (
	dsFlagCaveat = iota
	dsFlagExpiration
	dsFlagWildcard
)

func (r *DatastoreIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, DatastoreIteratorType, r.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, dsFlagCaveat, r.base.Caveat() != "")
		setFlag(&flags, dsFlagExpiration, r.base.Expiration())
		setFlag(&flags, dsFlagWildcard, r.base.Wildcard())
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		// Always-present identifying fields.
		if err := writeString(buf, r.base.DefinitionName()); err != nil {
			return err
		}
		if err := writeString(buf, r.base.RelationName()); err != nil {
			return err
		}
		if err := writeString(buf, r.base.Type()); err != nil {
			return err
		}
		if err := writeString(buf, r.base.Subrelation()); err != nil {
			return err
		}
		if hasFlag(flags, dsFlagCaveat) {
			if err := writeString(buf, r.base.Caveat()); err != nil {
				return err
			}
		}
		return nil
	})
}

func deserializeDatastore(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	if dctx == nil || dctx.Schema == nil {
		return nil, errors.New("DatastoreIterator deserialize requires DeserializeContext with Schema")
	}
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("datastore flags: %w", err)
	}
	defName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore def: %w", err)
	}
	relName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore rel: %w", err)
	}
	subjectType, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore subjectType: %w", err)
	}
	subrelation, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("datastore subrelation: %w", err)
	}
	var caveat string
	if hasFlag(flags, dsFlagCaveat) {
		if caveat, err = readString(br); err != nil {
			return nil, fmt.Errorf("datastore caveat: %w", err)
		}
	}
	base, err := dctx.Schema.ResolveBaseRelation(
		defName, relName, subjectType, subrelation, caveat,
		hasFlag(flags, dsFlagExpiration), hasFlag(flags, dsFlagWildcard),
	)
	if err != nil {
		return nil, fmt.Errorf("datastore: %w", err)
	}
	ds := NewDatastoreIterator(base)
	ds.canonicalKey = key
	return ds, nil
}

// ----- Fixed -----

const fixedFlagHasPaths = 0

func (f *FixedIterator) Serialize(w io.Writer) error {
	// Empty FixedIterator decompiles to NullIteratorType; emit Null on the wire
	// so the receiver reconstructs the same shape.
	if len(f.paths) == 0 {
		return serializeWithHeader(w, NullIteratorType, f.canonicalKey, func(buf io.Writer) error {
			return writeUvarint(buf, 0)
		})
	}
	return serializeWithHeader(w, FixedIteratorType, f.canonicalKey, func(buf io.Writer) error {
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

// ----- Path (used by FixedIterator) -----
//
// Path.Metadata is runtime-only (map[string]any with arbitrary values produced
// by the executor) and is intentionally not serialized — round-trip equality
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

// ----- Union -----

func (u *UnionIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, UnionIteratorType, u.canonicalKey, func(buf io.Writer) error {
		if err := writeUvarint(buf, 0); err != nil { // flags reserved
			return err
		}
		return writeSubs(buf, u.subIts)
	})
}

func deserializeUnion(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
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
	case *FixedIterator:
		v.canonicalKey = key
	}
	return it, nil
}

// ----- Intersection -----

func (i *IntersectionIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, IntersectionIteratorType, i.canonicalKey, func(buf io.Writer) error {
		if err := writeUvarint(buf, 0); err != nil {
			return err
		}
		return writeSubs(buf, i.subIts)
	})
}

func deserializeIntersection(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		return nil, fmt.Errorf("intersection flags: %w", err)
	}
	subs, err := readSubs(br, dctx)
	if err != nil {
		return nil, err
	}
	it := NewIntersectionIterator(subs...)
	switch v := it.(type) {
	case *IntersectionIterator:
		v.canonicalKey = key
	case *FixedIterator:
		v.canonicalKey = key
	}
	return it, nil
}

// ----- Arrow -----

const (
	arrowFlagSchemaArrow = iota
	arrowFlagRightToLeft
)

func (a *ArrowIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, ArrowIteratorType, a.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, arrowFlagSchemaArrow, a.isSchemaArrow)
		setFlag(&flags, arrowFlagRightToLeft, a.direction == rightToLeft)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if err := a.left.Serialize(buf); err != nil {
			return fmt.Errorf("left: %w", err)
		}
		if err := a.right.Serialize(buf); err != nil {
			return fmt.Errorf("right: %w", err)
		}
		return nil
	})
}

func deserializeArrow(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("arrow flags: %w", err)
	}
	subs, err := readNSubs(br, 2, dctx)
	if err != nil {
		return nil, err
	}
	a := &ArrowIterator{
		left:          subs[0],
		right:         subs[1],
		isSchemaArrow: hasFlag(flags, arrowFlagSchemaArrow),
		direction:     leftToRight,
		canonicalKey:  key,
	}
	if hasFlag(flags, arrowFlagRightToLeft) {
		a.direction = rightToLeft
	}
	return a, nil
}

// ----- Exclusion -----

func (e *ExclusionIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, ExclusionIteratorType, e.canonicalKey, func(buf io.Writer) error {
		if err := writeUvarint(buf, 0); err != nil {
			return err
		}
		if err := e.mainSet.Serialize(buf); err != nil {
			return fmt.Errorf("mainSet: %w", err)
		}
		if err := e.excluded.Serialize(buf); err != nil {
			return fmt.Errorf("excluded: %w", err)
		}
		return nil
	})
}

func deserializeExclusion(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		return nil, fmt.Errorf("exclusion flags: %w", err)
	}
	subs, err := readNSubs(br, 2, dctx)
	if err != nil {
		return nil, err
	}
	ex := NewExclusionIterator(subs[0], subs[1])
	ex.canonicalKey = key
	return ex, nil
}

// ----- Caveat -----

const caveatFlagHasCaveat = 0

func (c *CaveatIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, CaveatIteratorType, c.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, caveatFlagHasCaveat, c.caveat != nil)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if hasFlag(flags, caveatFlagHasCaveat) {
			if err := writeProto(buf, c.caveat); err != nil {
				return fmt.Errorf("caveat proto: %w", err)
			}
		}
		return c.subiterator.Serialize(buf)
	})
}

func deserializeCaveat(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("caveat flags: %w", err)
	}
	var caveat *core.ContextualizedCaveat
	if hasFlag(flags, caveatFlagHasCaveat) {
		c := &core.ContextualizedCaveat{}
		if err := readProto(br, c); err != nil {
			return nil, fmt.Errorf("caveat proto: %w", err)
		}
		caveat = c
	}
	sub, err := Deserialize(br, dctx)
	if err != nil {
		return nil, fmt.Errorf("caveat sub: %w", err)
	}
	if caveat == nil {
		return nil, errors.New("CaveatIterator requires non-nil caveat")
	}
	ci := NewCaveatIterator(sub, caveat)
	ci.canonicalKey = key
	return ci, nil
}

// ----- Alias -----

const aliasFlagHasAliasedAs = 0

func (a *AliasIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, AliasIteratorType, a.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, aliasFlagHasAliasedAs, len(a.aliasedAs) > 0)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if err := writeString(buf, a.definitionName); err != nil {
			return err
		}
		if err := writeString(buf, a.relation); err != nil {
			return err
		}
		if hasFlag(flags, aliasFlagHasAliasedAs) {
			if err := writeUvarint(buf, uint64(len(a.aliasedAs))); err != nil {
				return err
			}
			for _, n := range a.aliasedAs {
				if err := writeString(buf, n); err != nil {
					return err
				}
			}
		}
		return a.subIt.Serialize(buf)
	})
}

func deserializeAlias(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("alias flags: %w", err)
	}
	defName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("alias def: %w", err)
	}
	relName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("alias rel: %w", err)
	}
	var aliasedAs []string
	if hasFlag(flags, aliasFlagHasAliasedAs) {
		n, err := readUvarint(br)
		if err != nil {
			return nil, fmt.Errorf("alias aliasedAs count: %w", err)
		}
		if n > maxSubCount {
			return nil, fmt.Errorf("alias aliasedAs count %d exceeds %d", n, maxSubCount)
		}
		aliasedAs = make([]string, n)
		for i := range aliasedAs {
			s, err := readString(br)
			if err != nil {
				return nil, fmt.Errorf("alias aliasedAs[%d]: %w", i, err)
			}
			aliasedAs[i] = s
		}
	}
	sub, err := Deserialize(br, dctx)
	if err != nil {
		return nil, fmt.Errorf("alias sub: %w", err)
	}
	al := NewAliasIteratorWithChain(defName, relName, aliasedAs, sub)
	al.canonicalKey = key
	return al, nil
}

// ----- Recursive -----

const recursiveFlagStrategy = 0 // strategy byte follows if set (default == iter-subjects)

func (r *RecursiveIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, RecursiveIteratorType, r.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		nonDefault := r.checkStrategy != recursiveCheckIterSubjects
		setFlag(&flags, recursiveFlagStrategy, nonDefault)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if err := writeString(buf, r.definitionName); err != nil {
			return err
		}
		if err := writeString(buf, r.relationName); err != nil {
			return err
		}
		if nonDefault {
			if _, err := buf.Write([]byte{byte(r.checkStrategy)}); err != nil {
				return err
			}
		}
		return r.templateTree.Serialize(buf)
	})
}

func deserializeRecursive(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("recursive flags: %w", err)
	}
	defName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("recursive def: %w", err)
	}
	relName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("recursive rel: %w", err)
	}
	strategy := recursiveCheckIterSubjects
	if hasFlag(flags, recursiveFlagStrategy) {
		b, err := br.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("recursive strategy: %w", err)
		}
		strategy = recursiveCheckStrategy(b)
	}
	sub, err := Deserialize(br, dctx)
	if err != nil {
		return nil, fmt.Errorf("recursive template: %w", err)
	}
	ri := NewRecursiveIterator(sub, defName, relName)
	ri.checkStrategy = strategy
	ri.canonicalKey = key
	return ri, nil
}

// ----- RecursiveSentinel -----

const sentinelFlagWithSubRelations = 0

func (r *RecursiveSentinelIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, RecursiveSentinelIteratorType, r.canonicalKey, func(buf io.Writer) error {
		var flags uint64
		setFlag(&flags, sentinelFlagWithSubRelations, r.withSubRelations)
		if err := writeUvarint(buf, flags); err != nil {
			return err
		}
		if err := writeString(buf, r.definitionName); err != nil {
			return err
		}
		return writeString(buf, r.relationName)
	})
}

func deserializeRecursiveSentinel(body io.Reader, key CanonicalKey, _ *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	flags, err := readUvarint(br)
	if err != nil {
		return nil, fmt.Errorf("sentinel flags: %w", err)
	}
	defName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("sentinel def: %w", err)
	}
	relName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("sentinel rel: %w", err)
	}
	s := NewRecursiveSentinelIterator(defName, relName, hasFlag(flags, sentinelFlagWithSubRelations))
	s.canonicalKey = key
	return s, nil
}

// ----- IntersectionArrow -----

func (ia *IntersectionArrowIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, IntersectionArrowIteratorType, ia.canonicalKey, func(buf io.Writer) error {
		if err := writeUvarint(buf, 0); err != nil {
			return err
		}
		if err := ia.left.Serialize(buf); err != nil {
			return fmt.Errorf("left: %w", err)
		}
		if err := ia.right.Serialize(buf); err != nil {
			return fmt.Errorf("right: %w", err)
		}
		return nil
	})
}

func deserializeIntersectionArrow(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		return nil, fmt.Errorf("intersection_arrow flags: %w", err)
	}
	subs, err := readNSubs(br, 2, dctx)
	if err != nil {
		return nil, err
	}
	ia := NewIntersectionArrowIterator(subs[0], subs[1])
	ia.canonicalKey = key
	return ia, nil
}

// ----- Self -----

func (s *SelfIterator) Serialize(w io.Writer) error {
	return serializeWithHeader(w, SelfIteratorType, s.canonicalKey, func(buf io.Writer) error {
		if err := writeUvarint(buf, 0); err != nil {
			return err
		}
		if err := writeString(buf, s.typeName); err != nil {
			return err
		}
		return writeString(buf, s.relation)
	})
}

func deserializeSelf(body io.Reader, key CanonicalKey, _ *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		return nil, fmt.Errorf("self flags: %w", err)
	}
	typeName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("self typeName: %w", err)
	}
	rel, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("self relation: %w", err)
	}
	self := NewSelfIterator(rel, typeName)
	self.canonicalKey = key
	return self, nil
}
