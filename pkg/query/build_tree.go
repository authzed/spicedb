package query

import (
	"errors"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type recursiveSentinelInfo struct {
	sentinel       *RecursiveSentinel
	definitionName string
	relationName   string
}

type iteratorBuilder struct {
	schema             *schema.Schema
	building           map[string]bool              // Track what's currently being built (call stack)
	collectedCaveats   []*core.ContextualizedCaveat // Collect caveats to combine with AND logic
	recursiveSentinels []*recursiveSentinelInfo     // Track recursion points for wrapping in RecursiveIterator
}

// BuildIteratorFromSchema takes a schema and walks the schema tree for a given definition namespace and a relationship or
// permission therein. From this, it generates an iterator tree, rooted on that relationship.
func BuildIteratorFromSchema(fullSchema *schema.Schema, definitionName string, relationName string) (Iterator, error) {
	builder := &iteratorBuilder{
		schema:             fullSchema,
		building:           make(map[string]bool),
		collectedCaveats:   make([]*core.ContextualizedCaveat, 0),
		recursiveSentinels: make([]*recursiveSentinelInfo, 0),
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

	// Note: RecursiveIterator wrapping happens at the recursion point,
	// not at the top level. So we shouldn't have any sentinels left here.
	if len(builder.recursiveSentinels) > 0 {
		// This would be an error - sentinels should have been wrapped already
		return nil, spiceerrors.MustBugf("unwrapped sentinels remaining: %d", len(builder.recursiveSentinels))
	}

	return result, nil
}

func (b *iteratorBuilder) buildIteratorFromSchemaInternal(definitionName string, relationName string, withSubRelations bool) (Iterator, error) {
	id := fmt.Sprintf("%s#%s", definitionName, relationName)

	// Check if we're currently building this (true recursion)
	// Check both with the same flag and opposite flag, since recursion can cross the boundary
	if b.building[id] {
		// Recursion detected - create sentinel and remember where
		sentinel := NewRecursiveSentinel(definitionName, relationName, withSubRelations)
		// Track this sentinel with its location info
		sentinelInfo := &recursiveSentinelInfo{
			sentinel:       sentinel,
			definitionName: definitionName,
			relationName:   relationName,
		}
		b.recursiveSentinels = append(b.recursiveSentinels, sentinelInfo)
		return sentinel, nil
	}

	// Mark as currently building
	b.building[id] = true
	// Track the position in the sentinels list before building
	sentinelsLenBefore := len(b.recursiveSentinels)

	def, ok := b.schema.GetTypeDefinition(definitionName)
	if !ok {
		// Remove before returning error
		delete(b.building, id)
		return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find a schema definition named `%s`", definitionName)
	}

	var result Iterator
	var err error
	if p, ok := def.GetPermission(relationName); ok {
		result, err = b.buildIteratorFromPermission(p)
	} else if r, ok := def.GetRelation(relationName); ok {
		result, err = b.buildIteratorFromRelation(r, withSubRelations)
	} else {
		err = RelationNotFoundError{
			definitionName: definitionName,
			relationName:   relationName,
		}
	}

	// Remove from building after we're done (allows reuse in other branches)
	delete(b.building, id)

	if err != nil {
		return nil, err
	}

	// Check if any NEW sentinels were added while building this
	// If so, this subtree contains recursion and should be wrapped
	sentinelsAdded := b.recursiveSentinels[sentinelsLenBefore:]
	if len(sentinelsAdded) > 0 {
		// Filter sentinels to only include those matching this definition/relation
		// Non-matching sentinels are left in the list for parent builds to handle
		var matchingSentinels []*RecursiveSentinel
		var nonMatchingSentinels []*recursiveSentinelInfo

		for _, info := range sentinelsAdded {
			if info.definitionName == definitionName && info.relationName == relationName {
				matchingSentinels = append(matchingSentinels, info.sentinel)
			} else {
				nonMatchingSentinels = append(nonMatchingSentinels, info)
			}
		}

		// Only wrap if we have matching sentinels
		if len(matchingSentinels) > 0 {
			// Wrap this subtree in RecursiveIterator with the current definition and relation
			result = NewRecursiveIterator(result, definitionName, relationName)
		}

		// Remove matching sentinels from the list, but keep non-matching ones for parent
		b.recursiveSentinels = b.recursiveSentinels[:sentinelsLenBefore]
		b.recursiveSentinels = append(b.recursiveSentinels, nonMatchingSentinels...)
	}

	return result, nil
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
		rel, ok := p.Parent().GetRelation(perm.Left())
		if !ok {
			return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find left-hand relation for arrow `%s->%s` for permission `%s` in definition `%s`", perm.Left(), perm.Right(), p.Name(), p.Parent().Name())
		}
		return b.buildArrowIterators(rel, perm.Right())

	case *schema.NilReference:
		return NewEmptyFixedIterator(), nil

	case *schema.RelationReference:
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

	case *schema.FunctionedArrowReference:
		rel, ok := p.Parent().GetRelation(perm.Left())
		if !ok {
			return nil, fmt.Errorf("BuildIteratorFromSchema: couldn't find arrow relation `%s` for functioned arrow `%s.%s(%s)` for permission `%s` in definition `%s`", perm.Left(), perm.Left(), functionTypeString(perm.Function()), perm.Right(), p.Name(), p.Parent().Name())
		}

		switch perm.Function() {
		case schema.FunctionTypeAny:
			// any() functions just like an arrow
			return b.buildArrowIterators(rel, perm.Right())

		case schema.FunctionTypeAll:
			// all() requires intersection arrow - user must have permission on ALL left subjects
			return b.buildIntersectionArrowIterators(rel, perm.Right())

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

	// Check if we need to expand subrelations
	// We always need to expand if withSubRelations=true (normal case)
	// OR if the subrelation might be recursive (same type as something we're building)
	needsExpansion := withSubRelations

	if !needsExpansion {
		// Check if this might be a recursive subrelation
		// by seeing if the subrelation type matches any definition we're currently building
		subrelID := fmt.Sprintf("%s#%s", br.Type(), br.Subrelation())
		if b.building[subrelID] {
			// This is recursive! We need to expand to detect it
			needsExpansion = true
		}
	}

	if !needsExpansion {
		return base, nil
	}

	rightside, err := b.buildIteratorFromSchemaInternal(br.Type(), br.Subrelation(), false)
	if err != nil {
		return nil, err
	}

	// We must check the effective arrow of a subrelation if we have one
	arrow := NewArrow(base.Clone(), rightside)
	union := NewUnion(base, arrow)
	return union, nil
}

// buildArrowIterators creates a union of arrow iterators for the given relation and right-hand side
func (b *iteratorBuilder) buildArrowIterators(rel *schema.Relation, rightSide string) (Iterator, error) {
	union := NewUnion()
	hasMultipleBaseRelations := len(rel.BaseRelations()) > 1
	var lastNotFoundError error

	for _, br := range rel.BaseRelations() {
		left, err := b.buildBaseRelationIterator(br, false)
		if err != nil {
			return nil, err
		}
		right, err := b.buildIteratorFromSchemaInternal(br.Type(), rightSide, false)
		if err != nil {
			// If the right side doesn't exist on this type, the arrow produces an empty set.
			// This is valid when a relation has multiple types and the arrow only
			// applies to some of them. If there's only one base relation, we should error.
			if errors.As(err, &RelationNotFoundError{}) {
				if hasMultipleBaseRelations {
					union.addSubIterator(NewEmptyFixedIterator())
					continue
				}
				lastNotFoundError = err
				continue
			}
			return nil, err
		}
		arrow := NewArrow(left, right)
		union.addSubIterator(arrow)
	}

	// If we have no sub-iterators and only have a not-found error, return that error
	if len(union.Subiterators()) == 0 && lastNotFoundError != nil {
		return nil, lastNotFoundError
	}

	return union, nil
}

// buildIntersectionArrowIterators creates a union of intersection arrow iterators for the given relation and right-hand side
func (b *iteratorBuilder) buildIntersectionArrowIterators(rel *schema.Relation, rightSide string) (Iterator, error) {
	union := NewUnion()
	hasMultipleBaseRelations := len(rel.BaseRelations()) > 1
	var lastNotFoundError error

	for _, br := range rel.BaseRelations() {
		left, err := b.buildBaseRelationIterator(br, false)
		if err != nil {
			return nil, err
		}
		right, err := b.buildIteratorFromSchemaInternal(br.Type(), rightSide, false)
		if err != nil {
			// If the right side doesn't exist on this type, the intersection arrow produces an empty set.
			// This is valid when a relation has multiple types and the arrow only
			// applies to some of them. If there's only one base relation, we should error.
			if errors.As(err, &RelationNotFoundError{}) {
				if hasMultipleBaseRelations {
					union.addSubIterator(NewEmptyFixedIterator())
					continue
				}
				lastNotFoundError = err
				continue
			}
			return nil, err
		}
		intersectionArrow := NewIntersectionArrow(left, right)
		union.addSubIterator(intersectionArrow)
	}

	// If we have no sub-iterators and only have a not-found error, return that error
	if len(union.Subiterators()) == 0 && lastNotFoundError != nil {
		return nil, lastNotFoundError
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

// RelationNotFoundError is returned when a relation or permission is not found in a definition
type RelationNotFoundError struct {
	definitionName string
	relationName   string
}

func (e RelationNotFoundError) Error() string {
	return fmt.Sprintf("BuildIteratorFromSchema: couldn't find a relation or permission named `%s` in definition `%s`", e.relationName, e.definitionName)
}
