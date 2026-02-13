package query

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// IteratorType is an enum to represent each basic type of iterator by a
// well-known byte.
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

// Outline is a single type representing the tree of yet-to-be-compiled Iterators.
type Outline struct {
	Type         IteratorType
	Args         *IteratorArgs
	Subiterators []Outline
}

// IteratorArgs represents all the possible arguments to the Iterator constructors.
// It is used by the Outline to carry the context of an Iterator.
type IteratorArgs struct {
	Relation       *schema.BaseRelation
	DefinitionName string
	RelationName   string
	Caveat         *core.ContextualizedCaveat
	FixedPaths     []Path
}

// Compile converts a query Outline into the actual Iterator representation.
func (c Outline) Compile() (Iterator, error) {
	// First, recursively compile all subiterators (bottom-up)
	compiledSubs := make([]Iterator, len(c.Subiterators))
	for i, sub := range c.Subiterators {
		compiled, err := sub.Compile()
		if err != nil {
			return nil, err
		}
		compiledSubs[i] = compiled
	}

	// Now construct the iterator based on type
	switch c.Type {
	case NullIteratorType:
		return NewFixedIterator(), nil

	case DatastoreIteratorType:
		if c.Args == nil || c.Args.Relation == nil {
			return nil, fmt.Errorf("DatastoreIterator requires Relation in Args")
		}
		return NewDatastoreIterator(c.Args.Relation), nil

	case UnionIteratorType:
		return NewUnionIterator(compiledSubs...), nil

	case IntersectionIteratorType:
		return NewIntersectionIterator(compiledSubs...), nil

	case FixedIteratorType:
		// FixedIterator with no paths (would need additional args for paths)
		if c.Args != nil {
			return NewFixedIterator(c.Args.FixedPaths...), nil
		}
		return NewFixedIterator(), nil

	case ArrowIteratorType:
		if len(compiledSubs) != 2 {
			return nil, fmt.Errorf("ArrowIterator requires exactly 2 subiterators, got %d", len(compiledSubs))
		}
		return NewArrowIterator(compiledSubs[0], compiledSubs[1]), nil

	case ExclusionIteratorType:
		if len(compiledSubs) != 2 {
			return nil, fmt.Errorf("ExclusionIterator requires exactly 2 subiterators, got %d", len(compiledSubs))
		}
		return NewExclusionIterator(compiledSubs[0], compiledSubs[1]), nil

	case CaveatIteratorType:
		if len(compiledSubs) != 1 {
			return nil, fmt.Errorf("CaveatIterator requires exactly 1 subiterator, got %d", len(compiledSubs))
		}
		if c.Args == nil || c.Args.Caveat == nil {
			return nil, fmt.Errorf("CaveatIterator requires Caveat in Args")
		}
		return NewCaveatIterator(compiledSubs[0], c.Args.Caveat), nil

	case AliasIteratorType:
		if len(compiledSubs) != 1 {
			return nil, fmt.Errorf("AliasIterator requires exactly 1 subiterator, got %d", len(compiledSubs))
		}
		if c.Args == nil || c.Args.RelationName == "" {
			return nil, fmt.Errorf("AliasIterator requires RelationName in Args")
		}
		return NewAliasIterator(c.Args.RelationName, compiledSubs[0]), nil

	case RecursiveIteratorType:
		if len(compiledSubs) != 1 {
			return nil, fmt.Errorf("RecursiveIterator requires exactly 1 subiterator, got %d", len(compiledSubs))
		}
		if c.Args == nil || c.Args.DefinitionName == "" || c.Args.RelationName == "" {
			return nil, fmt.Errorf("RecursiveIterator requires DefinitionName and RelationName in Args")
		}
		return NewRecursiveIterator(compiledSubs[0], c.Args.DefinitionName, c.Args.RelationName), nil

	case RecursiveSentinelIteratorType:
		if c.Args == nil || c.Args.DefinitionName == "" || c.Args.RelationName == "" {
			return nil, fmt.Errorf("RecursiveSentinelIterator requires DefinitionName and RelationName in Args")
		}
		// withSubRelations defaults to false for now
		return NewRecursiveSentinelIterator(c.Args.DefinitionName, c.Args.RelationName, false), nil

	case IntersectionArrowIteratorType:
		if len(compiledSubs) != 2 {
			return nil, fmt.Errorf("IntersectionArrowIterator requires exactly 2 subiterators, got %d", len(compiledSubs))
		}
		return NewIntersectionArrowIterator(compiledSubs[0], compiledSubs[1]), nil

	case SelfIteratorType:
		if c.Args == nil || c.Args.RelationName == "" || c.Args.DefinitionName == "" {
			return nil, fmt.Errorf("SelfIterator requires RelationName and DefinitionName in Args")
		}
		return NewSelfIterator(c.Args.RelationName, c.Args.DefinitionName), nil

	default:
		return nil, fmt.Errorf("unknown iterator type: %c", c.Type)
	}
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
			Subiterators: decompSubs,
		}, nil

	case *UnionIterator:
		return Outline{
			Type:         UnionIteratorType,
			Subiterators: decompSubs,
		}, nil

	case *IntersectionIterator:
		return Outline{
			Type:         IntersectionIteratorType,
			Subiterators: decompSubs,
		}, nil

	case *FixedIterator:
		return Outline{
			Type: FixedIteratorType,
			Args: &IteratorArgs{
				FixedPaths: typed.paths,
			},
			Subiterators: decompSubs,
		}, nil

	case *ArrowIterator:
		return Outline{
			Type:         ArrowIteratorType,
			Subiterators: decompSubs,
		}, nil

	case *ExclusionIterator:
		return Outline{
			Type:         ExclusionIteratorType,
			Subiterators: decompSubs,
		}, nil

	case *CaveatIterator:
		return Outline{
			Type: CaveatIteratorType,
			Args: &IteratorArgs{
				Caveat: typed.caveat,
			},
			Subiterators: decompSubs,
		}, nil

	case *AliasIterator:
		return Outline{
			Type: AliasIteratorType,
			Args: &IteratorArgs{
				RelationName: typed.relation,
			},
			Subiterators: decompSubs,
		}, nil

	case *RecursiveIterator:
		return Outline{
			Type: RecursiveIteratorType,
			Args: &IteratorArgs{
				DefinitionName: typed.definitionName,
				RelationName:   typed.relationName,
			},
			Subiterators: decompSubs,
		}, nil

	case *RecursiveSentinelIterator:
		return Outline{
			Type: RecursiveSentinelIteratorType,
			Args: &IteratorArgs{
				DefinitionName: typed.definitionName,
				RelationName:   typed.relationName,
			},
			Subiterators: decompSubs,
		}, nil

	case *IntersectionArrowIterator:
		return Outline{
			Type:         IntersectionArrowIteratorType,
			Subiterators: decompSubs,
		}, nil

	case *SelfIterator:
		return Outline{
			Type: SelfIteratorType,
			Args: &IteratorArgs{
				RelationName:   typed.relation,
				DefinitionName: typed.typeName,
			},
			Subiterators: decompSubs,
		}, nil

	default:
		return Outline{}, fmt.Errorf("unknown iterator type: %T", it)
	}
}
