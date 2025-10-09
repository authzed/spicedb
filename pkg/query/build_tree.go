package query

import (
	"errors"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

type iteratorBuilder struct {
	schema           *schema.Schema
	seen             map[string]bool
	collectedCaveats []*core.ContextualizedCaveat // Collect caveats to combine with AND logic
}

// BuildIteratorFromSchema takes a schema and walks the schema tree for a given definition namespace and a relationship or
// permission therein. From this, it generates an iterator tree, rooted on that relationship.
func BuildIteratorFromSchema(fullSchema *schema.Schema, definitionName string, relationName string) (Iterator, error) {
	builder := &iteratorBuilder{
		schema:           fullSchema,
		seen:             make(map[string]bool),
		collectedCaveats: make([]*core.ContextualizedCaveat, 0),
	}
	iterator, err := builder.buildIteratorFromSchemaInternal(definitionName, relationName, true)
	if err != nil {
		return nil, err
	}

	// Apply collected caveats at top level as individual caveat iterators
	result := iterator
	for _, caveat := range builder.collectedCaveats {
		result = NewCaveatIterator(result, caveat)
	}
	return result, nil
}

func (b *iteratorBuilder) buildIteratorFromSchemaInternal(definitionName string, relationName string, withSubRelations bool) (Iterator, error) {
	id := fmt.Sprintf("%s#%s:%v", definitionName, relationName, withSubRelations)
	if b.seen[id] {
		return nil, errors.New("recursive schema iterators are as yet unsupported")
	}
	b.seen[id] = true

	def, ok := b.schema.Definitions()[definitionName]
	if !ok {
		return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find a schema definition named `%s`", definitionName)
	}
	if p, ok := def.Permissions()[relationName]; ok {
		return b.buildIteratorFromPermission(p)
	}
	if r, ok := def.Relations()[relationName]; ok {
		return b.buildIteratorFromRelation(r, withSubRelations)
	}
	return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find a relation or permission named `%s` in definition `%s`", relationName, definitionName)
}

func (b *iteratorBuilder) buildIteratorFromRelation(r *schema.Relation, withSubRelations bool) (Iterator, error) {
	if len(r.BaseRelations()) == 1 {
		baseIt, err := b.buildBaseRelationIterator(r.BaseRelations()[0], withSubRelations)
		if err != nil {
			return nil, err
		}
		return NewAlias(r.Name(), baseIt), nil
	}
	union := NewUnion()
	for _, br := range r.BaseRelations() {
		it, err := b.buildBaseRelationIterator(br, withSubRelations)
		if err != nil {
			return nil, err
		}
		union.addSubIterator(it)
	}
	return NewAlias(r.Name(), union), nil
}

func (b *iteratorBuilder) buildIteratorFromPermission(p *schema.Permission) (Iterator, error) {
	baseIt, err := b.buildIteratorFromOperation(p, p.Operation())
	if err != nil {
		return nil, err
	}
	return NewAlias(p.Name(), baseIt), nil
}

func (b *iteratorBuilder) buildIteratorFromOperation(p *schema.Permission, op schema.Operation) (Iterator, error) {
	switch perm := op.(type) {
	case *schema.ArrowReference:
		rel, ok := p.Parent().Relations()[perm.Left()]
		if !ok {
			return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find left-hand relation for arrow `%s->%s` for permission `%s` in definition `%s`", perm.Left(), perm.Right(), p.Name(), p.Parent().Name())
		}
		return b.buildArrowIterators(rel, perm.Right())

	case *schema.RelationReference:
		if perm.RelationName() == "_nil" {
			return NewEmptyFixedIterator(), nil
		}
		return b.buildIteratorFromSchemaInternal(p.Parent().Name(), perm.RelationName(), true)

	case *schema.UnionOperation:
		union := NewUnion()
		for _, op := range perm.Children() {
			it, err := b.buildIteratorFromOperation(p, op)
			if err != nil {
				return nil, err
			}
			union.addSubIterator(it)
		}
		return union, nil

	case *schema.IntersectionOperation:
		inter := NewIntersection()
		for _, op := range perm.Children() {
			it, err := b.buildIteratorFromOperation(p, op)
			if err != nil {
				return nil, err
			}
			inter.addSubIterator(it)
		}
		return inter, nil

	case *schema.ExclusionOperation:
		mainIt, err := b.buildIteratorFromOperation(p, perm.Left())
		if err != nil {
			return nil, err
		}

		excludedIt, err := b.buildIteratorFromOperation(p, perm.Right())
		if err != nil {
			return nil, err
		}

		return NewExclusion(mainIt, excludedIt), nil

	case *schema.FunctionedTuplesetOperation:
		rel, ok := p.Parent().Relations()[perm.TuplesetRelation()]
		if !ok {
			return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find tupleset relation `%s` for functioned tupleset `%s.%s(%s)` for permission `%s` in definition `%s`", perm.TuplesetRelation(), perm.TuplesetRelation(), functionTypeString(perm.Function()), perm.ComputedRelation(), p.Name(), p.Parent().Name())
		}

		switch perm.Function() {
		case schema.FunctionTypeAny:
			// any() functions just like an arrow
			return b.buildArrowIterators(rel, perm.ComputedRelation())

		case schema.FunctionTypeAll:
			// all() requires intersection arrow - user must have permission on ALL left subjects
			return b.buildIntersectionArrowIterators(rel, perm.ComputedRelation())

		default:
			return nil, fmt.Errorf("unknown function type: %v", perm.Function())
		}
	}

	return nil, fmt.Errorf("uncovered schema permission operation: %T", op)
}

func (b *iteratorBuilder) buildBaseRelationIterator(br *schema.BaseRelation, withSubRelations bool) (Iterator, error) {
	base := NewRelationIterator(br)

	// Collect caveat to apply at top level instead of wrapping immediately
	if br.Caveat() != "" {
		caveat := &core.ContextualizedCaveat{
			CaveatName: br.Caveat(),
			// Context will be provided at query time through the Context.CaveatContext
		}
		b.collectedCaveats = append(b.collectedCaveats, caveat)
	}

	if br.Subrelation() == tuple.Ellipsis {
		return base, nil
	}

	// If there's no subrelation (e.g., wildcards), just return the base iterator
	if br.Subrelation() == "" {
		return base, nil
	}

	// For relation references in schema definitions (like group#member in "relation member: user | group#member"),
	// we always need to resolve what the referenced relation means, even if withSubRelations=false.
	// The withSubRelations flag controls whether we build arrows for nested traversal, but relation
	// references in the schema definition itself must always be resolved.
	// However, we still need to prevent infinite recursion.
	if !withSubRelations {
		return base, nil
	}

	rightside, err := b.buildIteratorFromSchemaInternal(br.Type(), br.Subrelation(), false)
	if err != nil {
		return nil, err
	}

	// We must check the effective arrow of a subrelation if we have one and subrelations are enabled
	// (subrelations are disabled in cases of actual arrows)
	union := NewUnion()
	union.addSubIterator(base)

	arrow := NewArrow(base.Clone(), rightside)
	union.addSubIterator(arrow)
	return union, nil
}

// buildArrowIterators creates a union of arrow iterators for the given relation and right-hand side
func (b *iteratorBuilder) buildArrowIterators(rel *schema.Relation, rightSide string) (Iterator, error) {
	union := NewUnion()
	for _, br := range rel.BaseRelations() {
		left, err := b.buildBaseRelationIterator(br, false)
		if err != nil {
			return nil, err
		}
		right, err := b.buildIteratorFromSchemaInternal(br.Type(), rightSide, false)
		if err != nil {
			return nil, err
		}
		arrow := NewArrow(left, right)
		union.addSubIterator(arrow)
	}
	return union, nil
}

// buildIntersectionArrowIterators creates a union of intersection arrow iterators for the given relation and right-hand side
func (b *iteratorBuilder) buildIntersectionArrowIterators(rel *schema.Relation, rightSide string) (Iterator, error) {
	union := NewUnion()
	for _, br := range rel.BaseRelations() {
		left, err := b.buildBaseRelationIterator(br, false)
		if err != nil {
			return nil, err
		}
		right, err := b.buildIteratorFromSchemaInternal(br.Type(), rightSide, false)
		if err != nil {
			return nil, err
		}
		intersectionArrow := NewIntersectionArrow(left, right)
		union.addSubIterator(intersectionArrow)
	}
	return union, nil
}

func functionTypeString(ft schema.FunctionType) string {
	switch ft {
	case schema.FunctionTypeAny:
		return "any"
	case schema.FunctionTypeAll:
		return "all"
	default:
		return "unknown"
	}
}
