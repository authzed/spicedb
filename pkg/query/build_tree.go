package query

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type iteratorBuilder struct {
	schema *schema.Schema
	seen   map[string]bool
}

// BuildIteratorFromSchema takes a schema and walks the schema tree for a given definition namespace and a relationship or
// permission therein. From this, it generates an iterator tree, rooted on that relationship.
func BuildIteratorFromSchema(fullSchema *schema.Schema, definitionName string, relationName string) (Iterator, error) {
	builder := &iteratorBuilder{
		schema: fullSchema,
		seen:   make(map[string]bool),
	}
	return builder.buildIteratorFromSchemaInternal(definitionName, relationName, true)
}

func (b *iteratorBuilder) buildIteratorFromSchemaInternal(definitionName string, relationName string, withSubRelations bool) (Iterator, error) {
	id := fmt.Sprintf("%s#%s:%v", definitionName, relationName, withSubRelations)
	if b.seen[id] {
		return nil, fmt.Errorf("recursive schema iterators are as yet unsupported")
	}
	b.seen[id] = true

	def, ok := b.schema.Definitions[definitionName]
	if !ok {
		return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find a schema definition named `%s`", definitionName)
	}
	if p, ok := def.Permissions[relationName]; ok {
		return b.buildIteratorFromPermission(p)
	}
	if r, ok := def.Relations[relationName]; ok {
		return b.buildIteratorFromRelation(r, withSubRelations)
	}
	return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find a relation or permission named `%s` in definition `%s`", relationName, definitionName)
}

func (b *iteratorBuilder) buildIteratorFromRelation(r *schema.Relation, withSubRelations bool) (Iterator, error) {
	if len(r.BaseRelations) == 1 {
		return b.buildBaseRelationIterator(r.BaseRelations[0], withSubRelations)
	}
	union := NewUnion()
	for _, br := range r.BaseRelations {
		it, err := b.buildBaseRelationIterator(br, withSubRelations)
		if err != nil {
			return nil, err
		}
		union.addSubIterator(it)
	}
	return union, nil
}

func (b *iteratorBuilder) buildIteratorFromPermission(p *schema.Permission) (Iterator, error) {
	return b.buildIteratorFromOperation(p, p.Operation)
}

func (b *iteratorBuilder) buildIteratorFromOperation(p *schema.Permission, op schema.Operation) (Iterator, error) {
	switch perm := op.(type) {
	case *schema.ArrowReference:
		rel, ok := p.Parent.Relations[perm.Left]
		if !ok {
			return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find left-hand relation for arrow `%s->%s` for permission `%s` in definition `%s`", perm.Left, perm.Right, p.Name, p.Parent.Name)
		}
		union := NewUnion()
		for _, br := range rel.BaseRelations {
			left, err := b.buildBaseRelationIterator(br, false)
			if err != nil {
				return nil, err
			}
			right, err := b.buildIteratorFromSchemaInternal(br.Type, perm.Right, false)
			if err != nil {
				return nil, err
			}
			arrow := NewArrow(left, right)
			union.addSubIterator(arrow)
		}
		return union, nil

	case *schema.RelationReference:
		return b.buildIteratorFromSchemaInternal(p.Parent.Name, perm.RelationName, true)

	case *schema.UnionOperation:
		union := NewUnion()
		for _, op := range perm.Children {
			it, err := b.buildIteratorFromOperation(p, op)
			if err != nil {
				return nil, err
			}
			union.addSubIterator(it)
		}
		return union, nil

	case *schema.IntersectionOperation:
		inter := NewIntersection()
		for _, op := range perm.Children {
			it, err := b.buildIteratorFromOperation(p, op)
			if err != nil {
				return nil, err
			}
			inter.addSubIterator(it)
		}
		return inter, nil

	case *schema.ExclusionOperation:
		return nil, spiceerrors.MustBugf("unimplemented")
	}

	return nil, fmt.Errorf("uncovered schema permission operation: %T", op)
}

func (b *iteratorBuilder) buildBaseRelationIterator(br *schema.BaseRelation, withSubRelations bool) (Iterator, error) {
	base := NewRelationIterator(br)

	if !withSubRelations {
		return base, nil
	}

	if br.Subrelation == tuple.Ellipsis {
		return base, nil
	}

	// We must check the effective arrow of a subrelation if we have one and subrelations are enabled
	// (subrelations are disabled in cases of actual arrows)
	union := NewUnion()
	union.addSubIterator(base)

	rightside, err := b.buildIteratorFromSchemaInternal(br.Type, br.Subrelation, false)
	if err != nil {
		return nil, err
	}

	union.addSubIterator(NewArrow(base.Clone(), rightside))
	return union, nil
}
