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
	definitionName string
	relationName   string
}

type outlineBuilder struct {
	schema             *schema.Schema
	building           map[string]bool              // Track what's currently being built (call stack)
	collectedCaveats   []*core.ContextualizedCaveat // Collect caveats to combine with AND logic
	recursiveSentinels []*recursiveSentinelInfo     // Track recursion points for wrapping in RecursiveIterator
}

// BuildIteratorFromSchema takes a schema and walks the schema tree for a given definition namespace and a relationship or
// permission therein. From this, it generates an iterator tree, rooted on that relationship.
func BuildIteratorFromSchema(fullSchema *schema.Schema, definitionName string, relationName string) (Iterator, error) {
	outline, err := BuildOutlineFromSchema(fullSchema, definitionName, relationName)
	if err != nil {
		return nil, err
	}
	return outline.Compile()
}

// BuildOutlineFromSchema builds a Outline tree from the schema
func BuildOutlineFromSchema(fullSchema *schema.Schema, definitionName string, relationName string) (Outline, error) {
	builder := &outlineBuilder{
		schema:             fullSchema,
		building:           make(map[string]bool),
		collectedCaveats:   make([]*core.ContextualizedCaveat, 0),
		recursiveSentinels: make([]*recursiveSentinelInfo, 0),
	}
	outline, err := builder.buildOutlineFromSchemaInternal(definitionName, relationName, true)
	if err != nil {
		return Outline{}, err
	}

	// Apply collected caveats at top level as individual caveat iterators
	result := outline
	for _, caveat := range builder.collectedCaveats {
		result = Outline{
			Type:        CaveatIteratorType,
			Args:        &IteratorArgs{Caveat: caveat},
			SubOutlines: []Outline{result},
		}
	}

	// Note: RecursiveIterator wrapping happens at the recursion point,
	// not at the top level. So we shouldn't have any sentinels left here.
	if len(builder.recursiveSentinels) > 0 {
		// This would be an error - sentinels should have been wrapped already
		return Outline{}, spiceerrors.MustBugf("unwrapped sentinels remaining: %d", len(builder.recursiveSentinels))
	}

	return CanonicalizeOutline(result)
}

func (b *outlineBuilder) buildOutlineFromSchemaInternal(definitionName string, relationName string, withSubRelations bool) (Outline, error) {
	id := fmt.Sprintf("%s#%s", definitionName, relationName)

	// Check if we're currently building this (true recursion)
	if b.building[id] {
		// Recursion detected - create sentinel and remember where
		sentinelInfo := &recursiveSentinelInfo{
			definitionName: definitionName,
			relationName:   relationName,
		}
		b.recursiveSentinels = append(b.recursiveSentinels, sentinelInfo)
		return Outline{
			Type: RecursiveSentinelIteratorType,
			Args: &IteratorArgs{
				DefinitionName: definitionName,
				RelationName:   relationName,
			},
		}, nil
	}

	// Mark as currently building
	b.building[id] = true
	// Track the position in the sentinels list before building
	sentinelsLenBefore := len(b.recursiveSentinels)

	def, ok := b.schema.GetTypeDefinition(definitionName)
	if !ok {
		// Remove before returning error
		delete(b.building, id)
		return Outline{}, fmt.Errorf("BuildOutlineFromSchema: couldn't find a schema definition named `%s`", definitionName)
	}

	var result Outline
	var err error
	if p, ok := def.GetPermission(relationName); ok {
		result, err = b.buildOutlineFromPermission(p)
	} else if r, ok := def.GetRelation(relationName); ok {
		result, err = b.buildOutlineFromRelation(r, withSubRelations)
	} else {
		err = RelationNotFoundError{
			definitionName: definitionName,
			relationName:   relationName,
		}
	}

	// Remove from building after we're done (allows reuse in other branches)
	delete(b.building, id)

	if err != nil {
		return Outline{}, err
	}

	// Check if any NEW sentinels were added while building this
	// If so, this subtree contains recursion and should be wrapped
	sentinelsAdded := b.recursiveSentinels[sentinelsLenBefore:]
	if len(sentinelsAdded) > 0 {
		// Filter sentinels to only include those matching this definition/relation
		// Non-matching sentinels are left in the list for parent builds to handle
		var matchingCount int
		var nonMatchingSentinels []*recursiveSentinelInfo

		for _, info := range sentinelsAdded {
			if info.definitionName == definitionName && info.relationName == relationName {
				matchingCount++
			} else {
				nonMatchingSentinels = append(nonMatchingSentinels, info)
			}
		}

		// Only wrap if we have matching sentinels
		if matchingCount > 0 {
			// Wrap this subtree in RecursiveIterator with the current definition and relation
			result = Outline{
				Type: RecursiveIteratorType,
				Args: &IteratorArgs{
					DefinitionName: definitionName,
					RelationName:   relationName,
				},
				SubOutlines: []Outline{result},
			}
		}

		// Remove matching sentinels from the list, but keep non-matching ones for parent
		b.recursiveSentinels = b.recursiveSentinels[:sentinelsLenBefore]
		b.recursiveSentinels = append(b.recursiveSentinels, nonMatchingSentinels...)
	}

	return result, nil
}

func (b *outlineBuilder) buildOutlineFromRelation(r *schema.Relation, withSubRelations bool) (Outline, error) {
	if len(r.BaseRelations()) == 1 {
		baseIt, err := b.buildBaseDatastoreOutline(r.BaseRelations()[0], withSubRelations)
		if err != nil {
			return Outline{}, err
		}
		return Outline{
			Type:        AliasIteratorType,
			Args:        &IteratorArgs{RelationName: r.Name()},
			SubOutlines: []Outline{baseIt},
		}, nil
	}
	subIts := make([]Outline, 0, len(r.BaseRelations()))
	for _, br := range r.BaseRelations() {
		it, err := b.buildBaseDatastoreOutline(br, withSubRelations)
		if err != nil {
			return Outline{}, err
		}
		subIts = append(subIts, it)
	}
	union := Outline{
		Type:        UnionIteratorType,
		SubOutlines: subIts,
	}
	return Outline{
		Type:        AliasIteratorType,
		Args:        &IteratorArgs{RelationName: r.Name()},
		SubOutlines: []Outline{union},
	}, nil
}

func (b *outlineBuilder) buildOutlineFromPermission(p *schema.Permission) (Outline, error) {
	baseIt, err := b.buildOutlineFromOperation(p, p.Operation())
	if err != nil {
		return Outline{}, err
	}
	return Outline{
		Type:        AliasIteratorType,
		Args:        &IteratorArgs{RelationName: p.Name()},
		SubOutlines: []Outline{baseIt},
	}, nil
}

func (b *outlineBuilder) buildOutlineFromOperation(p *schema.Permission, op schema.Operation) (Outline, error) {
	parentDef := p.Definition()
	switch perm := op.(type) {
	case *schema.ArrowReference:
		rel, ok := parentDef.GetRelation(perm.Left())
		if !ok {
			return Outline{}, fmt.Errorf("BuildOutlineFromSchema: couldn't find left-hand relation for arrow `%s->%s` for permission `%s` in definition `%s`", perm.Left(), perm.Right(), p.Name(), parentDef.Name())
		}
		return b.buildArrowOutline(rel, perm.Right())

	case *schema.NilReference:
		return Outline{Type: FixedIteratorType}, nil

	case *schema.SelfReference:
		return Outline{
			Type: SelfIteratorType,
			Args: &IteratorArgs{
				RelationName:   p.Name(),
				DefinitionName: parentDef.Name(),
			},
		}, nil

	case *schema.RelationReference:
		return b.buildOutlineFromSchemaInternal(parentDef.Name(), perm.RelationName(), true)

	case *schema.UnionOperation:
		subIts := make([]Outline, 0, len(perm.Children()))
		for _, op := range perm.Children() {
			it, err := b.buildOutlineFromOperation(p, op)
			if err != nil {
				return Outline{}, err
			}
			subIts = append(subIts, it)
		}
		return Outline{
			Type:        UnionIteratorType,
			SubOutlines: subIts,
		}, nil

	case *schema.IntersectionOperation:
		subIts := make([]Outline, 0, len(perm.Children()))
		for _, op := range perm.Children() {
			it, err := b.buildOutlineFromOperation(p, op)
			if err != nil {
				return Outline{}, err
			}
			subIts = append(subIts, it)
		}
		return Outline{
			Type:        IntersectionIteratorType,
			SubOutlines: subIts,
		}, nil

	case *schema.ExclusionOperation:
		mainIt, err := b.buildOutlineFromOperation(p, perm.Left())
		if err != nil {
			return Outline{}, err
		}

		excludedIt, err := b.buildOutlineFromOperation(p, perm.Right())
		if err != nil {
			return Outline{}, err
		}

		return Outline{
			Type:        ExclusionIteratorType,
			SubOutlines: []Outline{mainIt, excludedIt},
		}, nil

	case *schema.FunctionedArrowReference:
		rel, ok := parentDef.GetRelation(perm.Left())
		if !ok {
			return Outline{}, fmt.Errorf("BuildOutlineFromSchema: couldn't find arrow relation `%s` for functioned arrow `%s.%s(%s)` for permission `%s` in definition `%s`", perm.Left(), perm.Left(), functionTypeString(perm.Function()), perm.Right(), p.Name(), parentDef.Name())
		}

		switch perm.Function() {
		case schema.FunctionTypeAny:
			// any() functions just like an arrow
			return b.buildArrowOutline(rel, perm.Right())

		case schema.FunctionTypeAll:
			// all() requires intersection arrow - user must have permission on ALL left subjects
			return b.buildIntersectionArrowOutline(rel, perm.Right())

		default:
			return Outline{}, fmt.Errorf("unknown function type: %v", perm.Function())
		}
	}

	return Outline{}, fmt.Errorf("uncovered schema permission operation: %T", op)
}

func (b *outlineBuilder) buildBaseDatastoreOutline(br *schema.BaseRelation, withSubRelations bool) (Outline, error) {
	base := Outline{
		Type: DatastoreIteratorType,
		Args: &IteratorArgs{Relation: br},
	}

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
	needsExpansion := withSubRelations

	if !needsExpansion {
		// Check if this might be a recursive subrelation
		subrelID := fmt.Sprintf("%s#%s", br.Type(), br.Subrelation())
		if b.building[subrelID] {
			// This is recursive! We need to expand to detect it
			needsExpansion = true
		}
	}

	if !needsExpansion {
		return base, nil
	}

	rightside, err := b.buildOutlineFromSchemaInternal(br.Type(), br.Subrelation(), false)
	if err != nil {
		return Outline{}, err
	}

	// We must check the effective arrow of a subrelation if we have one
	arrow := Outline{
		Type:        ArrowIteratorType,
		SubOutlines: []Outline{base, rightside},
	}
	union := Outline{
		Type:        UnionIteratorType,
		SubOutlines: []Outline{base, arrow},
	}
	return union, nil
}

// buildArrowOutline creates a union of arrow iterators for the given relation and right-hand side
func (b *outlineBuilder) buildArrowOutline(rel *schema.Relation, rightSide string) (Outline, error) {
	subIts := make([]Outline, 0, len(rel.BaseRelations()))
	hasMultipleBaseRelations := len(rel.BaseRelations()) > 1
	var lastNotFoundError error

	for _, br := range rel.BaseRelations() {
		left, err := b.buildBaseDatastoreOutline(br, false)
		if err != nil {
			return Outline{}, err
		}
		right, err := b.buildOutlineFromSchemaInternal(br.Type(), rightSide, false)
		if err != nil {
			// If the right side doesn't exist on this type, the arrow produces an empty set.
			if errors.As(err, &RelationNotFoundError{}) {
				if hasMultipleBaseRelations {
					subIts = append(subIts, Outline{Type: FixedIteratorType})
					continue
				}
				lastNotFoundError = err
				continue
			}
			return Outline{}, err
		}
		// Create arrow (both specific subrelations and ellipsis use ArrowIteratorType)
		// The difference is handled by NewSchemaArrow vs NewArrowIterator during compilation
		arrow := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{left, right},
		}
		subIts = append(subIts, arrow)
	}

	// If we have no sub-iterators and only have a not-found error, return that error
	if len(subIts) == 0 && lastNotFoundError != nil {
		return Outline{}, lastNotFoundError
	}

	return Outline{
		Type:        UnionIteratorType,
		SubOutlines: subIts,
	}, nil
}

// buildIntersectionArrowOutline creates a union of intersection arrow iterators
func (b *outlineBuilder) buildIntersectionArrowOutline(rel *schema.Relation, rightSide string) (Outline, error) {
	subIts := make([]Outline, 0, len(rel.BaseRelations()))
	hasMultipleBaseRelations := len(rel.BaseRelations()) > 1
	var lastNotFoundError error

	for _, br := range rel.BaseRelations() {
		left, err := b.buildBaseDatastoreOutline(br, false)
		if err != nil {
			return Outline{}, err
		}
		right, err := b.buildOutlineFromSchemaInternal(br.Type(), rightSide, false)
		if err != nil {
			// If the right side doesn't exist on this type, the intersection arrow produces an empty set.
			if errors.As(err, &RelationNotFoundError{}) {
				if hasMultipleBaseRelations {
					subIts = append(subIts, Outline{Type: FixedIteratorType})
					continue
				}
				lastNotFoundError = err
				continue
			}
			return Outline{}, err
		}
		intersectionArrow := Outline{
			Type:        IntersectionArrowIteratorType,
			SubOutlines: []Outline{left, right},
		}
		subIts = append(subIts, intersectionArrow)
	}

	// If we have no sub-iterators and only have a not-found error, return that error
	if len(subIts) == 0 && lastNotFoundError != nil {
		return Outline{}, lastNotFoundError
	}

	return Outline{
		Type:        UnionIteratorType,
		SubOutlines: subIts,
	}, nil
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
	return fmt.Sprintf("BuildOutlineFromSchema: couldn't find a relation or permission named `%s` in definition `%s`", e.relationName, e.definitionName)
}
