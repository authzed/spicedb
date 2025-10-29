package schema

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/dalzilio/rudd"
)

// FlattenedSchema wraps a ResolvedSchema where all nested operations under permissions
// have been replaced with references to synthetic permissions.
type FlattenedSchema struct {
	resolvedSchema *ResolvedSchema
}

// ResolvedSchema returns the underlying resolved schema with flattened operations.
func (f *FlattenedSchema) ResolvedSchema() *ResolvedSchema {
	return f.resolvedSchema
}

// WalkFlattenedSchema walks the flattened schema tree, calling appropriate visitor methods
// on the provided Visitor for each node encountered. This is a convenience function that
// delegates to WalkSchema on the underlying schema.
func WalkFlattenedSchema[T any](fs *FlattenedSchema, v Visitor[T], value T) (T, error) {
	if fs == nil {
		return value, nil
	}
	return WalkSchema(fs.resolvedSchema.schema, v, value)
}

// FlattenSeparator is the separator used between permission names and hashes in synthetic permissions.
type FlattenSeparator string

const (
	// FlattenSeparatorDollar uses $ as separator (e.g., view$abc123).
	// Note: $ is not valid in schema DSL identifiers, so this is only for internal use.
	FlattenSeparatorDollar FlattenSeparator = "$"

	// FlattenSeparatorDoubleUnderscore uses __ as separator (e.g., view__abc123).
	// This is valid in schema DSL identifiers.
	FlattenSeparatorDoubleUnderscore FlattenSeparator = "__"
)

// FlattenOptions contains options for flattening a schema.
type FlattenOptions struct {
	// Separator controls what separator is used between permission names and hashes
	// in synthetic permissions ($ or __).
	Separator FlattenSeparator

	// FlattenNonUnionOperations controls whether non-union operations (intersections,
	// exclusions) should be flattened. When true, nested compound operations are
	// extracted into synthetic permissions.
	FlattenNonUnionOperations bool

	// FlattenArrows controls whether arrow operations (->) should be flattened.
	// When true, arrow operations are treated as leaf nodes and are extracted into
	// their own synthetic permissions. When false, they remain as-is in the operation tree.
	FlattenArrows bool
}

// FlattenSchema takes a resolved schema and recursively flattens all nested operations
// under each permission's root expression by replacing them with references to new
// synthetic permissions. The synthetic permissions are named using the pattern:
// `{permissionName}{separator}{hash}` where hash is computed using rudd BDD canonicalization.
// The separator parameter controls what separator is used ($ or __).
//
// This function uses default flattening options that flatten both non-union operations
// and arrows.
func FlattenSchema(rs *ResolvedSchema, separator FlattenSeparator) (*FlattenedSchema, error) {
	return FlattenSchemaWithOptions(rs, FlattenOptions{
		Separator:                 separator,
		FlattenNonUnionOperations: true,
		FlattenArrows:             true,
	})
}

// FlattenSchemaWithOptions takes a resolved schema and recursively flattens operations
// according to the provided options. Nested operations under each permission's root
// expression are replaced with references to new synthetic permissions. The synthetic
// permissions are named using the pattern: `{permissionName}{separator}{hash}` where
// hash is computed using rudd BDD canonicalization.
func FlattenSchemaWithOptions(rs *ResolvedSchema, options FlattenOptions) (*FlattenedSchema, error) {
	if rs == nil {
		return nil, errors.New("cannot flatten nil resolved schema")
	}

	// Clone the schema so we don't modify the original
	schema := rs.schema.clone()

	// Walk through all definitions and flatten their permissions
	for _, def := range schema.definitions {
		if err := flattenDefinition(def, options); err != nil {
			return nil, fmt.Errorf("failed to flatten definition %s: %w", def.name, err)
		}
	}

	return &FlattenedSchema{
		resolvedSchema: &ResolvedSchema{schema: schema},
	}, nil
}

// flattenDefinition recursively flattens all permissions in a definition.
func flattenDefinition(def *Definition, options FlattenOptions) error {
	// Process all permissions
	for _, perm := range def.permissions {
		flattened, newPerms, err := flattenOperation(perm.operation, def, perm.name, options)
		if err != nil {
			return fmt.Errorf("failed to flatten permission %s: %w", perm.name, err)
		}

		// Update the permission's operation to the flattened version
		perm.operation = flattened

		// Add any new synthetic permissions to the definition
		for _, newPerm := range newPerms {
			def.permissions[newPerm.name] = newPerm
		}
	}

	return nil
}

// flattenOperation recursively flattens an operation tree, replacing nested operations
// with references to synthetic permissions.
// Returns: (flattened operation, list of new synthetic permissions, error)
func flattenOperation(op Operation, def *Definition, baseName string, options FlattenOptions) (Operation, []*Permission, error) {
	if op == nil {
		return nil, nil, nil
	}

	var allNewPerms []*Permission

	switch o := op.(type) {
	case *ResolvedRelationReference:
		// Leaf node, but we need to update the resolved reference to point to the cloned schema
		// Check if this references a relation or permission in the definition
		if rel, ok := o.resolved.(*Relation); ok {
			// Find the corresponding relation in the cloned definition
			if clonedRel, exists := def.relations[rel.name]; exists {
				return &ResolvedRelationReference{
					relationName: o.relationName,
					resolved:     clonedRel,
				}, nil, nil
			}
		} else if perm, ok := o.resolved.(*Permission); ok {
			// Find the corresponding permission in the cloned definition
			if clonedPerm, exists := def.permissions[perm.name]; exists {
				return &ResolvedRelationReference{
					relationName: o.relationName,
					resolved:     clonedPerm,
				}, nil, nil
			}
		}
		// If we can't find the referenced object, just return the original reference
		// This shouldn't happen in a properly resolved schema
		return o, nil, nil

	case *ResolvedArrowReference:
		// Arrows are always leaf nodes in the flattened tree
		// They will be extracted when they appear as children of operations if FlattenArrows is enabled
		return o, nil, nil

	case *ResolvedFunctionedArrowReference:
		// Functioned arrows are always leaf nodes in the flattened tree
		// They will be extracted when they appear as children of operations if FlattenArrows is enabled
		return o, nil, nil

	case *RelationReference:
		// Unresolved leaf node, no flattening needed
		return o, nil, nil

	case *ArrowReference:
		// Arrows are always leaf nodes in the flattened tree
		// They will be extracted when they appear as children of operations if FlattenArrows is enabled
		return o, nil, nil

	case *FunctionedArrowReference:
		// Functioned arrows are always leaf nodes in the flattened tree
		// They will be extracted when they appear as children of operations if FlattenArrows is enabled
		return o, nil, nil

	case *NilReference:
		// Nil reference is a leaf node, no flattening needed
		return o, nil, nil

	case *UnionOperation:
		// Flatten children first
		flattenedChildren := make([]Operation, len(o.children))
		for i, child := range o.children {
			if isNestedOperation(child, options) {
				// Create a synthetic permission for this nested operation
				synthPerm, newPerms, err := createSyntheticPermission(child, def, baseName, options)
				if err != nil {
					return nil, nil, err
				}
				flattenedChildren[i] = &ResolvedRelationReference{
					relationName: synthPerm.name,
					resolved:     synthPerm,
				}
				allNewPerms = append(allNewPerms, synthPerm)
				allNewPerms = append(allNewPerms, newPerms...)
			} else {
				flattened, newPerms, err := flattenOperation(child, def, baseName, options)
				if err != nil {
					return nil, nil, err
				}
				flattenedChildren[i] = flattened
				allNewPerms = append(allNewPerms, newPerms...)
			}
		}
		return &UnionOperation{children: flattenedChildren}, allNewPerms, nil

	case *IntersectionOperation:
		// Flatten children first
		flattenedChildren := make([]Operation, len(o.children))
		for i, child := range o.children {
			if isNestedOperation(child, options) {
				// Create a synthetic permission for this nested operation
				synthPerm, newPerms, err := createSyntheticPermission(child, def, baseName, options)
				if err != nil {
					return nil, nil, err
				}
				flattenedChildren[i] = &ResolvedRelationReference{
					relationName: synthPerm.name,
					resolved:     synthPerm,
				}
				allNewPerms = append(allNewPerms, synthPerm)
				allNewPerms = append(allNewPerms, newPerms...)
			} else {
				flattened, newPerms, err := flattenOperation(child, def, baseName, options)
				if err != nil {
					return nil, nil, err
				}
				flattenedChildren[i] = flattened
				allNewPerms = append(allNewPerms, newPerms...)
			}
		}
		return &IntersectionOperation{children: flattenedChildren}, allNewPerms, nil

	case *ExclusionOperation:
		// Flatten left side
		var flattenedLeft Operation
		if isNestedOperation(o.left, options) {
			synthPerm, newPerms, err := createSyntheticPermission(o.left, def, baseName, options)
			if err != nil {
				return nil, nil, err
			}
			flattenedLeft = &ResolvedRelationReference{
				relationName: synthPerm.name,
				resolved:     synthPerm,
			}
			allNewPerms = append(allNewPerms, synthPerm)
			allNewPerms = append(allNewPerms, newPerms...)
		} else {
			flattened, newPerms, err := flattenOperation(o.left, def, baseName, options)
			if err != nil {
				return nil, nil, err
			}
			flattenedLeft = flattened
			allNewPerms = append(allNewPerms, newPerms...)
		}

		// Flatten right side
		var flattenedRight Operation
		if isNestedOperation(o.right, options) {
			synthPerm, newPerms, err := createSyntheticPermission(o.right, def, baseName, options)
			if err != nil {
				return nil, nil, err
			}
			flattenedRight = &ResolvedRelationReference{
				relationName: synthPerm.name,
				resolved:     synthPerm,
			}
			allNewPerms = append(allNewPerms, synthPerm)
			allNewPerms = append(allNewPerms, newPerms...)
		} else {
			flattened, newPerms, err := flattenOperation(o.right, def, baseName, options)
			if err != nil {
				return nil, nil, err
			}
			flattenedRight = flattened
			allNewPerms = append(allNewPerms, newPerms...)
		}

		return &ExclusionOperation{
			left:  flattenedLeft,
			right: flattenedRight,
		}, allNewPerms, nil

	default:
		return nil, nil, fmt.Errorf("unknown operation type: %T", op)
	}
}

// isNestedOperation returns true if the operation should be extracted into a synthetic permission
// based on the flatten options.
func isNestedOperation(op Operation, options FlattenOptions) bool {
	switch op.(type) {
	case *UnionOperation, *IntersectionOperation, *ExclusionOperation:
		return options.FlattenNonUnionOperations
	case *ResolvedArrowReference, *ArrowReference, *ResolvedFunctionedArrowReference, *FunctionedArrowReference:
		return options.FlattenArrows
	default:
		return false
	}
}

// createSyntheticPermission creates a new synthetic permission for the given nested operation.
// It recursively flattens the operation and computes a hash-based name.
func createSyntheticPermission(op Operation, def *Definition, baseName string, options FlattenOptions) (*Permission, []*Permission, error) {
	// Recursively flatten the operation
	flattened, newPerms, err := flattenOperation(op, def, baseName, options)
	if err != nil {
		return nil, nil, err
	}

	// Compute hash for the operation
	hash, err := computeOperationHash(flattened)
	if err != nil {
		return nil, nil, err
	}

	// Create the synthetic permission name
	synthName := fmt.Sprintf("%s%s%s", baseName, options.Separator, hash)

	// Check if a synthetic permission with this name already exists
	if existingPerm, exists := def.permissions[synthName]; exists {
		// Reuse the existing permission instead of creating a duplicate
		return existingPerm, newPerms, nil
	}

	// Create the synthetic permission
	synthPerm := &Permission{
		parent:    def,
		name:      synthName,
		operation: flattened,
		synthetic: true,
	}

	return synthPerm, newPerms, nil
}

// computeOperationHash computes a hash of an operation using rudd BDD canonicalization.
func computeOperationHash(op Operation) (string, error) {
	// Build a variable map for this operation
	varMap := buildOperationVarMap(op)

	if len(varMap) == 0 {
		// No variables, return a simple hash
		hasher := fnv.New64a()
		hasher.Write([]byte("empty"))
		return hex.EncodeToString(hasher.Sum(nil)), nil
	}

	// Create BDD with the number of variables
	bdd, err := rudd.New(len(varMap))
	if err != nil {
		return "", fmt.Errorf("failed to create BDD: %w", err)
	}

	// Convert operation to BDD
	node, err := operationToBdd(op, bdd, varMap)
	if err != nil {
		return "", fmt.Errorf("failed to convert operation to BDD: %w", err)
	}

	// Hash the BDD structure
	hasher := fnv.New64a()
	bdd.Print(hasher, node)

	// Also include the variable names in the hash to distinguish between
	// structurally identical operations with different variables
	// Sort the variable names for deterministic hashing
	varNames := make([]string, len(varMap))
	for name, idx := range varMap {
		varNames[idx] = name
	}
	for _, name := range varNames {
		hasher.Write([]byte(name))
		hasher.Write([]byte{0}) // separator
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// buildOperationVarMap builds a map from relation/arrow names to variable indices.
func buildOperationVarMap(op Operation) map[string]int {
	varMap := make(map[string]int)
	buildVarMapRecursive(op, varMap)
	return varMap
}

func buildVarMapRecursive(op Operation, varMap map[string]int) {
	if op == nil {
		return
	}

	switch o := op.(type) {
	case *ResolvedRelationReference:
		if _, ok := varMap[o.relationName]; !ok {
			varMap[o.relationName] = len(varMap)
		}
	case *RelationReference:
		if _, ok := varMap[o.relationName]; !ok {
			varMap[o.relationName] = len(varMap)
		}
	case *ResolvedArrowReference:
		key := o.left + "->" + o.right
		if _, ok := varMap[key]; !ok {
			varMap[key] = len(varMap)
		}
	case *ArrowReference:
		key := o.left + "->" + o.right
		if _, ok := varMap[key]; !ok {
			varMap[key] = len(varMap)
		}
	case *ResolvedFunctionedArrowReference:
		// "any" functioned arrows are equivalent to regular arrows for BDD purposes
		// "all" functioned arrows are different and use a distinct key
		var key string
		if o.function == FunctionTypeAll {
			key = o.left + ".all(" + o.right + ")"
		} else {
			// FunctionTypeAny uses the same key as regular arrows
			key = o.left + "->" + o.right
		}
		if _, ok := varMap[key]; !ok {
			varMap[key] = len(varMap)
		}
	case *FunctionedArrowReference:
		// "any" functioned arrows are equivalent to regular arrows for BDD purposes
		// "all" functioned arrows are different and use a distinct key
		var key string
		if o.function == FunctionTypeAll {
			key = o.left + ".all(" + o.right + ")"
		} else {
			// FunctionTypeAny uses the same key as regular arrows
			key = o.left + "->" + o.right
		}
		if _, ok := varMap[key]; !ok {
			varMap[key] = len(varMap)
		}
	case *UnionOperation:
		for _, child := range o.children {
			buildVarMapRecursive(child, varMap)
		}
	case *IntersectionOperation:
		for _, child := range o.children {
			buildVarMapRecursive(child, varMap)
		}
	case *ExclusionOperation:
		buildVarMapRecursive(o.left, varMap)
		buildVarMapRecursive(o.right, varMap)
	case *NilReference:
		// NilReference has no variables to add to the map
	}
}

// operationToBdd converts an operation to a BDD node.
func operationToBdd(op Operation, bdd *rudd.BDD, varMap map[string]int) (rudd.Node, error) {
	if op == nil {
		return nil, errors.New("nil operation")
	}

	switch o := op.(type) {
	case *ResolvedRelationReference:
		idx, ok := varMap[o.relationName]
		if !ok {
			return nil, fmt.Errorf("relation %s not in varMap", o.relationName)
		}
		return bdd.Ithvar(idx), nil

	case *RelationReference:
		idx, ok := varMap[o.relationName]
		if !ok {
			return nil, fmt.Errorf("relation %s not in varMap", o.relationName)
		}
		return bdd.Ithvar(idx), nil

	case *ResolvedArrowReference:
		key := o.left + "->" + o.right
		idx, ok := varMap[key]
		if !ok {
			return nil, fmt.Errorf("arrow %s not in varMap", key)
		}
		return bdd.Ithvar(idx), nil

	case *ArrowReference:
		key := o.left + "->" + o.right
		idx, ok := varMap[key]
		if !ok {
			return nil, fmt.Errorf("arrow %s not in varMap", key)
		}
		return bdd.Ithvar(idx), nil

	case *ResolvedFunctionedArrowReference:
		// "any" functioned arrows are equivalent to regular arrows for BDD purposes
		// "all" functioned arrows are different and use a distinct key
		var key string
		if o.function == FunctionTypeAll {
			key = o.left + ".all(" + o.right + ")"
		} else {
			// FunctionTypeAny uses the same key as regular arrows
			key = o.left + "->" + o.right
		}
		idx, ok := varMap[key]
		if !ok {
			return nil, fmt.Errorf("functioned arrow %s not in varMap", key)
		}
		return bdd.Ithvar(idx), nil

	case *FunctionedArrowReference:
		// "any" functioned arrows are equivalent to regular arrows for BDD purposes
		// "all" functioned arrows are different and use a distinct key
		var key string
		if o.function == FunctionTypeAll {
			key = o.left + ".all(" + o.right + ")"
		} else {
			// FunctionTypeAny uses the same key as regular arrows
			key = o.left + "->" + o.right
		}
		idx, ok := varMap[key]
		if !ok {
			return nil, fmt.Errorf("functioned arrow %s not in varMap", key)
		}
		return bdd.Ithvar(idx), nil

	case *UnionOperation:
		nodes := make([]rudd.Node, len(o.children))
		for i, child := range o.children {
			node, err := operationToBdd(child, bdd, varMap)
			if err != nil {
				return nil, err
			}
			nodes[i] = node
		}
		return bdd.Or(nodes...), nil

	case *IntersectionOperation:
		nodes := make([]rudd.Node, len(o.children))
		for i, child := range o.children {
			node, err := operationToBdd(child, bdd, varMap)
			if err != nil {
				return nil, err
			}
			nodes[i] = node
		}
		return bdd.And(nodes...), nil

	case *ExclusionOperation:
		leftNode, err := operationToBdd(o.left, bdd, varMap)
		if err != nil {
			return nil, err
		}
		rightNode, err := operationToBdd(o.right, bdd, varMap)
		if err != nil {
			return nil, err
		}
		// Exclusion is: left AND NOT right
		// We need to negate the right node
		notRight := bdd.Not(rightNode)
		return bdd.And(leftNode, notRight), nil

	case *NilReference:
		// NilReference represents an empty set, so return the BDD false node
		return bdd.False(), nil

	default:
		return nil, fmt.Errorf("unknown operation type: %T", op)
	}
}
