package schema

import (
	"errors"
	"fmt"
)

// ResolvedSchema wraps a Schema where all RelationReferences and ArrowReferences
// have been resolved to their actual RelationOrPermission targets.
type ResolvedSchema struct {
	schema *Schema
}

// Schema returns the underlying resolved schema.
func (r *ResolvedSchema) Schema() *Schema {
	return r.schema
}

// WalkResolvedSchema walks the resolved schema tree, calling appropriate visitor methods
// on the provided Visitor for each node encountered. This is a convenience function that
// delegates to WalkSchema on the underlying schema.
func WalkResolvedSchema[T any](rs *ResolvedSchema, v Visitor[T], value T) (T, error) {
	if rs == nil {
		return value, nil
	}
	return WalkSchema(rs.schema, v, value)
}

// ResolveSchema takes a schema, clones it, walks through all operations,
// and replaces RelationReference and ArrowReference nodes with their resolved versions.
// Returns an error if any relation or arrow left side cannot be resolved.
func ResolveSchema(s *Schema) (*ResolvedSchema, error) {
	if s == nil {
		return nil, errors.New("cannot resolve nil schema")
	}

	// Clone the schema first so we don't modify the original
	cloned := s.clone()

	// Walk through all definitions and resolve their permissions
	for _, def := range cloned.definitions {
		for _, perm := range def.permissions {
			resolvedOp, err := resolveOperation(perm.operation, def)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve permission %s in definition %s: %w", perm.name, def.name, err)
			}
			perm.operation = resolvedOp
		}
	}

	return &ResolvedSchema{schema: cloned}, nil
}

// resolveOperation recursively resolves all RelationReferences and ArrowReferences
// in an operation tree, replacing them with their resolved counterparts.
func resolveOperation(op Operation, def *Definition) (Operation, error) {
	if op == nil {
		return nil, nil
	}

	switch o := op.(type) {
	case *RelationReference:
		// Look up the relation or permission in the current definition
		resolved := resolveRelationOrPermission(o.relationName, def)
		if resolved == nil {
			return nil, fmt.Errorf("relation or permission '%s' not found in definition '%s'", o.relationName, def.name)
		}
		return &ResolvedRelationReference{
			relationName: o.relationName,
			resolved:     resolved,
		}, nil

	case *ArrowReference:
		// Look up the left side relation in the current definition
		relation, ok := def.relations[o.left]
		if !ok {
			return nil, fmt.Errorf("relation '%s' not found in definition '%s' (left side of arrow)", o.left, def.name)
		}
		return &ResolvedArrowReference{
			left:         o.left,
			resolvedLeft: relation,
			right:        o.right,
		}, nil

	case *FunctionedArrowReference:
		// Look up the left side relation in the current definition
		relation, ok := def.relations[o.left]
		if !ok {
			return nil, fmt.Errorf("relation '%s' not found in definition '%s' (left side of functioned arrow)", o.left, def.name)
		}
		return &ResolvedFunctionedArrowReference{
			left:         o.left,
			resolvedLeft: relation,
			right:        o.right,
			function:     o.function,
		}, nil

	case *UnionOperation:
		children := make([]Operation, len(o.children))
		for i, child := range o.children {
			resolved, err := resolveOperation(child, def)
			if err != nil {
				return nil, err
			}
			children[i] = resolved
		}
		return &UnionOperation{
			children: children,
		}, nil

	case *IntersectionOperation:
		children := make([]Operation, len(o.children))
		for i, child := range o.children {
			resolved, err := resolveOperation(child, def)
			if err != nil {
				return nil, err
			}
			children[i] = resolved
		}
		return &IntersectionOperation{
			children: children,
		}, nil

	case *ExclusionOperation:
		leftResolved, err := resolveOperation(o.left, def)
		if err != nil {
			return nil, err
		}
		rightResolved, err := resolveOperation(o.right, def)
		if err != nil {
			return nil, err
		}
		return &ExclusionOperation{
			left:  leftResolved,
			right: rightResolved,
		}, nil

	case *ResolvedRelationReference:
		// Already resolved, just return it
		return o, nil

	case *ResolvedArrowReference:
		// Already resolved, just return it
		return o, nil

	case *ResolvedFunctionedArrowReference:
		// Already resolved, just return it
		return o, nil

	case *NilReference:
		// NilReference is not replaced during resolution
		return o, nil

	default:
		return nil, fmt.Errorf("unknown operation type: %T", op)
	}
}

// resolveRelationOrPermission looks up a relation or permission by name in the given definition.
func resolveRelationOrPermission(name string, def *Definition) RelationOrPermission {
	// Check relations first
	if rel, ok := def.relations[name]; ok {
		return rel
	}
	// Then check permissions
	if perm, ok := def.permissions[name]; ok {
		return perm
	}
	return nil
}
