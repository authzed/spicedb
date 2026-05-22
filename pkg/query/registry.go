package query

import (
	"io"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// IteratorType is an enum to represent each basic type of iterator by a
// well-known byte.
//
// Below we provide constants for the built-in iterator types. The registry will
// complain if you try to register a type of the same byte ID from outside this package.
type IteratorType byte

const (
	NullIteratorType              IteratorType = '0'
	DatastoreIteratorType         IteratorType = 'D'
	UnionIteratorType             IteratorType = '|'
	IntersectionIteratorType      IteratorType = '&'
	FixedIteratorType             IteratorType = 'F'
	ArrowIteratorType             IteratorType = '>'
	ExclusionIteratorType         IteratorType = 'X'
	CaveatIteratorType            IteratorType = 'C'
	AliasIteratorType             IteratorType = '@'
	RecursiveIteratorType         IteratorType = 'R'
	RecursiveSentinelIteratorType IteratorType = 'r'
	IntersectionArrowIteratorType IteratorType = 'A'
	SelfIteratorType              IteratorType = '='
)

// IteratorSpec describes how to construct a single concrete Iterator type from
// an Outline node. Each iterator implementation registers a spec via
// MustRegisterIterator in its file's init(), which lets the compile step build
// iterator trees without knowing the concrete types involved. External packages
// can register their own iterators by adding a spec for a new IteratorType.
type IteratorSpec struct {
	Type              IteratorType
	Name              string
	ConstructWithArgs func(args *IteratorArgs, subIterators []Iterator, key CanonicalKey) (Iterator, error)
	// Deserialize reads a single body from r (the type byte and the wrapping
	// key+bodyLen framing have already been peeled by the package-level
	// query.Deserialize). It returns the fully-reconstructed Iterator, including
	// any sub-iterators, which it reads recursively by calling query.Deserialize.
	// The CanonicalKey is supplied separately so the body need only carry the
	// type-specific args and children.
	Deserialize func(body io.Reader, key CanonicalKey, dctx *DeserializeContext) (Iterator, error)
}

var iteratorRegistry = make(map[IteratorType]IteratorSpec)

// MustRegisterIterator registers an IteratorSpec for a given IteratorType. It
// panics if a spec is already registered for that type, which is what we want
// at init() time so collisions are caught at startup.
func MustRegisterIterator(spec IteratorSpec) {
	if _, ok := iteratorRegistry[spec.Type]; ok {
		spiceerrors.MustPanicf("writing twice to the iteratorRegistry! type:`%s`, name:`%s`", string(spec.Type), spec.Name)
	}
	iteratorRegistry[spec.Type] = spec
}

// MakeIterator constructs an Iterator of the given type using the registered
// spec, threading the canonical key through to the constructor.
func MakeIterator(t IteratorType, args *IteratorArgs, subs []Iterator, key CanonicalKey) (Iterator, error) {
	if v, ok := iteratorRegistry[t]; ok {
		return v.ConstructWithArgs(args, subs, key)
	}
	return nil, spiceerrors.MustBugf("cannot find iterator of type `%s`", string(t))
}
