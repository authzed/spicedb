package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cespare/xxhash/v2"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// IteratorType is an enum to represent each basic type of iterator by a
// well-known byte.
//
// Remember to also update  the allIteratorTypes list below when adding a new one.
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

// CanonicalKey is a unique string identifier for a canonical Outline subtree.
// It is computed after canonicalization and represents the entire structure
// in a compact, deterministic format.
type CanonicalKey string

// String returns the string representation of the key
func (k CanonicalKey) String() string {
	return string(k)
}

// IsEmpty returns true if the key is empty (not yet computed)
func (k CanonicalKey) IsEmpty() bool {
	return string(k) == ""
}

// Hash returns a hash fingerprint of the key for use in maps
func (k CanonicalKey) Hash() uint64 {
	// Use xxhash for fast, non-cryptographic hashing
	return xxhash.Sum64String(string(k))
}

// CanonicalKeySource can resolve a CanonicalKey for a given outline node ID.
// PlanAdvisors receive this instead of the full CanonicalOutline to stay
// decoupled from the outline structure.
type CanonicalKeySource interface {
	GetCanonicalKey(id OutlineNodeID) CanonicalKey
}

// OutlineNodeID is a numeric identifier assigned to each node in a CanonicalOutline.
// It is populated by CanonicalizeOutline; plain (non-canonical, non-filter) Outlines have a
// zero-valued ID and cannot be compiled.
type OutlineNodeID uint64

// Outline is a single type representing the tree of yet-to-be-compiled Iterators.
type Outline struct {
	Type        IteratorType
	Args        *IteratorArgs
	SubOutlines []Outline
	ID          OutlineNodeID // Populated only inside a CanonicalOutline
}

// WalkOutlineBottomUp performs a bottom-up traversal of an Outline tree,
// rebuilding each node with its processed children before passing it to fn.
// fn receives the node (with already-processed children) and returns a
// replacement node and an optional error. The traversal stops and the error
// is propagated on the first failure.
func WalkOutlineBottomUp(outline Outline, fn func(Outline) (Outline, error)) (Outline, error) {
	if len(outline.SubOutlines) > 0 {
		newSubs := make([]Outline, len(outline.SubOutlines))
		for i, sub := range outline.SubOutlines {
			processed, err := WalkOutlineBottomUp(sub, fn)
			if err != nil {
				return Outline{}, err
			}
			newSubs[i] = processed
		}
		outline = Outline{
			Type:        outline.Type,
			Args:        outline.Args,
			SubOutlines: newSubs,
			ID:          outline.ID,
		}
	}
	return fn(outline)
}

// WalkOutlinePreOrder performs a pre-order traversal of an Outline tree,
// calling fn on each node before visiting its children. fn returns an error
// to abort the traversal early.
func WalkOutlinePreOrder(outline Outline, fn func(Outline) error) error {
	if err := fn(outline); err != nil {
		return err
	}
	for _, sub := range outline.SubOutlines {
		if err := WalkOutlinePreOrder(sub, fn); err != nil {
			return err
		}
	}
	return nil
}

// CanonicalOutline is an Outline tree that has been fully canonicalized.
// It pairs the root Outline (with every node's ID assigned) with a map from
// those IDs to their CanonicalKeys. Only CanonicalOutlines can be Compiled,
// ensuring every iterator in the resulting tree receives its canonical key.
type CanonicalOutline struct {
	Root          Outline
	CanonicalKeys map[OutlineNodeID]CanonicalKey
	Hints         map[OutlineNodeID][]Hint
}

// GetCanonicalKey implements CanonicalKeySource.
func (co CanonicalOutline) GetCanonicalKey(id OutlineNodeID) CanonicalKey {
	return co.CanonicalKeys[id]
}

// Compile converts a CanonicalOutline into the actual Iterator representation.
// All iterators in the resulting tree have their canonical keys set and hints applied.
func (co CanonicalOutline) Compile() (Iterator, error) {
	return compileOutline(co.Root, co.CanonicalKeys, co.Hints)
}

// compileOutline recursively builds an Iterator tree from an Outline,
// looking up each node's CanonicalKey from the provided map and applying hints.
func compileOutline(outline Outline, keys map[OutlineNodeID]CanonicalKey, hints map[OutlineNodeID][]Hint) (Iterator, error) {
	key, ok := keys[outline.ID]
	if !ok {
		return nil, spiceerrors.MustBugf("outline node ID %d not found in CanonicalKeys map - outline must come from a CanonicalOutline", outline.ID)
	}

	// First, recursively compile all subiterators (bottom-up)
	compiledSubs := make([]Iterator, len(outline.SubOutlines))
	for i, sub := range outline.SubOutlines {
		compiled, err := compileOutline(sub, keys, hints)
		if err != nil {
			return nil, err
		}
		compiledSubs[i] = compiled
	}

	// Now construct the iterator based on type and set canonical key
	var it Iterator
	switch outline.Type {
	case NullIteratorType:
		fixed := NewFixedIterator()
		fixed.canonicalKey = key
		it = fixed

	case DatastoreIteratorType:
		if outline.Args == nil || outline.Args.Relation == nil {
			return nil, errors.New("DatastoreIterator requires Relation in Args")
		}
		ds := NewDatastoreIterator(outline.Args.Relation)
		ds.canonicalKey = key
		it = ds

	case UnionIteratorType:
		union := NewUnionIterator(compiledSubs...)
		union.(*UnionIterator).canonicalKey = key
		it = union

	case IntersectionIteratorType:
		intersection := NewIntersectionIterator(compiledSubs...)
		intersection.(*IntersectionIterator).canonicalKey = key
		it = intersection

	case FixedIteratorType:
		var fixed *FixedIterator
		if outline.Args != nil {
			fixed = NewFixedIterator(outline.Args.FixedPaths...)
		} else {
			fixed = NewFixedIterator()
		}
		fixed.canonicalKey = key
		it = fixed

	case ArrowIteratorType:
		if len(compiledSubs) != 2 {
			return nil, fmt.Errorf("ArrowIterator requires exactly 2 subiterators, got %d", len(compiledSubs))
		}
		arrow := NewArrowIterator(compiledSubs[0], compiledSubs[1])
		arrow.canonicalKey = key
		it = arrow

	case ExclusionIteratorType:
		if len(compiledSubs) != 2 {
			return nil, fmt.Errorf("ExclusionIterator requires exactly 2 subiterators, got %d", len(compiledSubs))
		}
		exclusion := NewExclusionIterator(compiledSubs[0], compiledSubs[1])
		exclusion.canonicalKey = key
		it = exclusion

	case CaveatIteratorType:
		if len(compiledSubs) != 1 {
			return nil, fmt.Errorf("CaveatIterator requires exactly 1 subiterator, got %d", len(compiledSubs))
		}
		if outline.Args == nil || outline.Args.Caveat == nil {
			return nil, errors.New("CaveatIterator requires Caveat in Args")
		}
		caveat := NewCaveatIterator(compiledSubs[0], outline.Args.Caveat)
		caveat.canonicalKey = key
		it = caveat

	case AliasIteratorType:
		if len(compiledSubs) != 1 {
			return nil, fmt.Errorf("AliasIterator requires exactly 1 subiterator, got %d", len(compiledSubs))
		}
		if outline.Args == nil || outline.Args.RelationName == "" {
			return nil, errors.New("AliasIterator requires RelationName in Args")
		}
		alias := NewAliasIteratorWithChain(outline.Args.DefinitionName, outline.Args.RelationName, outline.Args.AliasedAs, compiledSubs[0])
		alias.canonicalKey = key
		it = alias

	case RecursiveIteratorType:
		if len(compiledSubs) != 1 {
			return nil, fmt.Errorf("RecursiveIterator requires exactly 1 subiterator, got %d", len(compiledSubs))
		}
		if outline.Args == nil || outline.Args.DefinitionName == "" || outline.Args.RelationName == "" {
			return nil, errors.New("RecursiveIterator requires DefinitionName and RelationName in Args")
		}
		recursive := NewRecursiveIterator(compiledSubs[0], outline.Args.DefinitionName, outline.Args.RelationName)
		recursive.canonicalKey = key
		it = recursive

	case RecursiveSentinelIteratorType:
		if outline.Args == nil || outline.Args.DefinitionName == "" || outline.Args.RelationName == "" {
			return nil, errors.New("RecursiveSentinelIterator requires DefinitionName and RelationName in Args")
		}
		// withSubRelations defaults to false for now
		sentinel := NewRecursiveSentinelIterator(outline.Args.DefinitionName, outline.Args.RelationName, false)
		sentinel.canonicalKey = key
		it = sentinel

	case IntersectionArrowIteratorType:
		if len(compiledSubs) != 2 {
			return nil, fmt.Errorf("IntersectionArrowIterator requires exactly 2 subiterators, got %d", len(compiledSubs))
		}
		intersectionArrow := NewIntersectionArrowIterator(compiledSubs[0], compiledSubs[1])
		intersectionArrow.canonicalKey = key
		it = intersectionArrow

	case SelfIteratorType:
		if outline.Args == nil || outline.Args.RelationName == "" || outline.Args.DefinitionName == "" {
			return nil, errors.New("SelfIterator requires RelationName and DefinitionName in Args")
		}
		self := NewSelfIterator(outline.Args.RelationName, outline.Args.DefinitionName)
		self.canonicalKey = key
		it = self

	default:
		return nil, fmt.Errorf("unknown iterator type: %c", outline.Type)
	}

	// Apply hints to the constructed iterator
	if hints != nil {
		for _, hint := range hints[outline.ID] {
			if err := hint(it); err != nil {
				return nil, err
			}
		}
	}

	return it, nil
}

// IteratorArgs represents all the possible arguments to the Iterator constructors.
// It is used by the Outline to carry the context of an Iterator.
type IteratorArgs struct {
	Relation       *schema.BaseRelation
	DefinitionName string
	RelationName   string
	// AliasedAs, when non-empty on an AliasIteratorType outline, lists the names
	// of additional outer aliases that were collapsed into this node, in
	// inner-to-outer order. The outermost name (last entry) is what emitted paths
	// are rewritten to; the self-edge fires for any subject relation in the chain
	// (RelationName ∪ AliasedAs). RelationName remains the alias's underlying
	// identity for caching. Set by the alias-chain-collapse optimizer.
	AliasedAs  []string
	Caveat     *core.ContextualizedCaveat
	FixedPaths []Path
}

// Decompile converts an Iterator back to its Outline representation
func Decompile(it Iterator) (Outline, error) {
	if it == nil {
		return Outline{Type: NullIteratorType}, nil
	}

	// Recursively decompile subiterators
	subs := it.Subiterators()
	decompSubs := make([]Outline, len(subs))
	for i, sub := range subs {
		decomp, err := Decompile(sub)
		if err != nil {
			return Outline{}, err
		}
		decompSubs[i] = decomp
	}

	// Type switch to extract arguments and determine type
	switch typed := it.(type) {
	case *DatastoreIterator:
		return Outline{
			Type: DatastoreIteratorType,
			Args: &IteratorArgs{
				Relation: typed.base,
			},
			SubOutlines: decompSubs,
		}, nil

	case *UnionIterator:
		return Outline{
			Type:        UnionIteratorType,
			SubOutlines: decompSubs,
		}, nil

	case *IntersectionIterator:
		return Outline{
			Type:        IntersectionIteratorType,
			SubOutlines: decompSubs,
		}, nil

	case *FixedIterator:
		// Empty FixedIterator is represented as NullIteratorType
		if len(typed.paths) == 0 {
			return Outline{
				Type:        NullIteratorType,
				SubOutlines: decompSubs,
			}, nil
		}
		return Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: typed.paths,
			},
			SubOutlines: decompSubs,
		}, nil

	case *ArrowIterator:
		return Outline{
			Type:        ArrowIteratorType,
			SubOutlines: decompSubs,
		}, nil

	case *ExclusionIterator:
		return Outline{
			Type:        ExclusionIteratorType,
			SubOutlines: decompSubs,
		}, nil

	case *CaveatIterator:
		return Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{
				Caveat: typed.caveat,
			},
			SubOutlines: decompSubs,
		}, nil

	case *AliasIterator:
		return Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{
				DefinitionName: typed.definitionName,
				RelationName:   typed.relation,
				AliasedAs:      append([]string(nil), typed.aliasedAs...),
			},
			SubOutlines: decompSubs,
		}, nil

	case *RecursiveIterator:
		return Outline{
			Type: RecursiveIteratorType,
			Args: &IteratorArgs{
				DefinitionName: typed.definitionName,
				RelationName:   typed.relationName,
			},
			SubOutlines: decompSubs,
		}, nil

	case *RecursiveSentinelIterator:
		return Outline{
			Type: RecursiveSentinelIteratorType,
			Args: &IteratorArgs{
				DefinitionName: typed.definitionName,
				RelationName:   typed.relationName,
			},
			SubOutlines: decompSubs,
		}, nil

	case *IntersectionArrowIterator:
		return Outline{
			Type:        IntersectionArrowIteratorType,
			SubOutlines: decompSubs,
		}, nil

	case *SelfIterator:
		return Outline{
			Type: SelfIteratorType,
			Args: &IteratorArgs{
				RelationName:   typed.relation,
				DefinitionName: typed.typeName,
			},
			SubOutlines: decompSubs,
		}, nil

	default:
		return Outline{}, fmt.Errorf("unknown iterator type: %T", it)
	}
}

// Equals checks if two Outlines are structurally equal
func (outline Outline) Equals(other Outline) bool {
	return OutlineCompare(outline, other) == 0
}

// OutlineCompare defines a total ordering on Outline for canonicalization.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Compatible with slices.SortFunc.
func OutlineCompare(a, b Outline) int {
	// First compare by type
	if a.Type != b.Type {
		if a.Type < b.Type {
			return -1
		}
		return 1
	}

	// Then compare by args
	argsCmp := argsCompare(a.Args, b.Args)
	if argsCmp != 0 {
		return argsCmp
	}

	// Then compare by number of subiterators
	if len(a.SubOutlines) != len(b.SubOutlines) {
		if len(a.SubOutlines) < len(b.SubOutlines) {
			return -1
		}
		return 1
	}

	// Finally, lexicographic comparison of subiterators
	for i := range a.SubOutlines {
		if !a.SubOutlines[i].Equals(b.SubOutlines[i]) {
			return OutlineCompare(a.SubOutlines[i], b.SubOutlines[i])
		}
	}

	return 0 // Equal
}

// argsCompare returns -1 if a < b, 0 if a == b, 1 if a > b
func argsCompare(a, b *IteratorArgs) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Compare DefinitionName
	if a.DefinitionName != b.DefinitionName {
		if a.DefinitionName < b.DefinitionName {
			return -1
		}
		return 1
	}

	// Compare RelationName
	if a.RelationName != b.RelationName {
		if a.RelationName < b.RelationName {
			return -1
		}
		return 1
	}

	// Compare AliasedAs
	if len(a.AliasedAs) != len(b.AliasedAs) {
		if len(a.AliasedAs) < len(b.AliasedAs) {
			return -1
		}
		return 1
	}
	for i := range a.AliasedAs {
		if a.AliasedAs[i] != b.AliasedAs[i] {
			if a.AliasedAs[i] < b.AliasedAs[i] {
				return -1
			}
			return 1
		}
	}

	// Compare Relation (BaseRelation)
	switch {
	case a.Relation != nil && b.Relation != nil:
		if cmp := a.Relation.Compare(b.Relation); cmp != 0 {
			return cmp
		}
	case a.Relation != nil:
		return 1
	case b.Relation != nil:
		return -1
	}

	// Compare Caveat
	if cmp := caveatCompare(a.Caveat, b.Caveat); cmp != 0 {
		return cmp
	}

	// Compare FixedPaths length
	if len(a.FixedPaths) != len(b.FixedPaths) {
		if len(a.FixedPaths) < len(b.FixedPaths) {
			return -1
		}
		return 1
	}

	// Compare FixedPaths lexicographically
	for i := range a.FixedPaths {
		if cmp := PathOrder(a.FixedPaths[i], b.FixedPaths[i]); cmp != 0 {
			return cmp
		}
	}

	return 0
}

// caveatCompare compares two caveats, returns -1, 0, or 1
// For ordering purposes, we compare by name first, then use EqualVT for tie-breaking
func caveatCompare(a, b *core.ContextualizedCaveat) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// First compare by caveat name for a stable ordering
	if a.CaveatName != b.CaveatName {
		if a.CaveatName < b.CaveatName {
			return -1
		}
		return 1
	}

	// If names are equal, use EqualVT to check if they're truly equal
	// If not equal, we need some deterministic ordering for the context
	if a.EqualVT(b) {
		return 0
	}

	// For different contexts with same name, compare context string representation
	// This ensures a stable (though arbitrary) ordering
	aCtx := a.Context.String()
	bCtx := b.Context.String()
	if aCtx != bCtx {
		if aCtx < bCtx {
			return -1
		}
		return 1
	}

	return 0
}

// Serialize generates a compact, deterministic string representation
// of an Outline subtree based on its Type, Args, and SubOutlines.
// The ID field is not included in serialization.
// Format: <Type>(<Args>)[<Sub1>,<Sub2>,...]
// Returns a CanonicalKey wrapping the serialized string.
//
// CaveatIterator nodes are identified solely by their caveat name, independent
// of their position in the tree. Their subiterators are not included in the key.
func (outline Outline) Serialize() CanonicalKey {
	var result strings.Builder

	// Add type (single character)
	result.WriteByte(byte(outline.Type))

	// Add args if present
	if outline.Args != nil {
		argsStr := serializeArgs(outline.Args)
		if argsStr != "" {
			result.WriteByte('(')
			result.WriteString(argsStr)
			result.WriteByte(')')
		}
	}

	// Caveat nodes are identified solely by their name, independent of
	// their position in the tree. Do not include subiterators in the key.
	if outline.Type == CaveatIteratorType {
		return CanonicalKey(result.String())
	}

	// Add subiterators if present
	if len(outline.SubOutlines) > 0 {
		result.WriteByte('[')
		for i, sub := range outline.SubOutlines {
			if i > 0 {
				result.WriteString(",")
			}
			result.WriteString(sub.Serialize().String())
		}
		result.WriteByte(']')
	}

	return CanonicalKey(result.String())
}

// serializeArgs converts IteratorArgs to compact string representation
// Format: field1:val1,field2:val2,...
// Fields in deterministic order: DefinitionName, RelationName, Relation, Caveat, FixedPaths
func serializeArgs(args *IteratorArgs) string {
	if args == nil {
		return ""
	}

	var sb strings.Builder

	// DefinitionName
	if args.DefinitionName != "" {
		sb.WriteString("def:")
		sb.WriteString(args.DefinitionName)
	}

	// RelationName
	if args.RelationName != "" {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("rel:")
		sb.WriteString(args.RelationName)
	}

	// AliasedAs (chain of outer alias names from inner-to-outer)
	if len(args.AliasedAs) > 0 {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("aas:")
		for i, n := range args.AliasedAs {
			if i > 0 {
				sb.WriteByte('|')
			}
			sb.WriteString(n)
		}
	}

	// Relation (BaseRelation)
	if args.Relation != nil {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("base:")
		sb.WriteString(serializeRelation(args.Relation))
	}

	// Caveat
	if args.Caveat != nil {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("cav:")
		sb.WriteString(serializeCaveat(args.Caveat))
	}

	// FixedPaths (count only)
	if len(args.FixedPaths) > 0 {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(serializePaths(args.FixedPaths))
	}

	return sb.String()
}

// serializeRelation converts BaseRelation to compact string
// Format: defName/relName/subjectType/subrel with flags /c (caveat), /e (expiration), /w (wildcard)
func serializeRelation(rel *schema.BaseRelation) string {
	if rel == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(rel.DefinitionName())
	sb.WriteByte('/')
	sb.WriteString(rel.RelationName())
	sb.WriteByte('/')
	sb.WriteString(rel.Type())
	sb.WriteByte('/')
	sb.WriteString(rel.Subrelation())

	if rel.Caveat() != "" {
		sb.WriteString("/c:")
		sb.WriteString(rel.Caveat())
	}
	if rel.Expiration() {
		sb.WriteString("/e")
	}
	if rel.Wildcard() {
		sb.WriteString("/w")
	}

	return sb.String()
}

// serializeCaveat converts ContextualizedCaveat to compact string (name only)
// Context is too verbose, so we only include the name
func serializeCaveat(caveat *core.ContextualizedCaveat) string {
	if caveat == nil {
		return ""
	}
	return caveat.CaveatName
}

// serializePaths converts FixedPaths to compact representation (count only)
// Full paths are too verbose, so we only include the count
func serializePaths(paths []Path) string {
	return fmt.Sprintf("paths:%d", len(paths))
}
