package schema

import "errors"

// WalkStrategy determines the order in which nodes are visited during traversal.
type WalkStrategy int

const (
	// WalkPreOrder visits each node before visiting its children.
	// This is the default traversal order.
	WalkPreOrder WalkStrategy = iota

	// WalkPostOrder visits each node's children before visiting the node itself.
	WalkPostOrder
)

// WalkOptions configures how the walk traversal is performed.
// Use NewWalkOptions() to create an instance and chain With* methods to configure.
type WalkOptions struct {
	// strategy determines the traversal order (pre-order or post-order).
	strategy WalkStrategy

	// traverseArrowTargets enables automatic traversal of arrow reference targets
	// during PostOrder traversal.
	traverseArrowTargets bool

	// schema is the root schema, used for resolving arrow targets when traverseArrowTargets is enabled.
	schema *Schema

	// visitedArrowTargets tracks which permissions have been visited during arrow traversal
	// to prevent infinite recursion in cyclic schemas. Internal use only.
	visitedArrowTargets map[string]bool
}

// NewWalkOptions creates a new WalkOptions with default settings (PreOrder strategy).
func NewWalkOptions() WalkOptions {
	return WalkOptions{
		strategy: WalkPreOrder,
	}
}

// WithStrategy sets the traversal strategy (PreOrder or PostOrder).
func (opts WalkOptions) WithStrategy(strategy WalkStrategy) WalkOptions {
	opts.strategy = strategy
	return opts
}

// WithTraverseArrowTargets enables automatic traversal of arrow reference targets.
//
// Requirements:
//   - PostOrder strategy (set via WithStrategy)
//   - Resolved schema (call ResolveSchema() before walking)
//   - Schema parameter for target lookup
//
// The schema parameter is the root schema used to resolve arrow targets.
// It MUST be a resolved schema (from ResolveSchema()) - if the schema contains
// unresolved ArrowReference nodes, the walk will fail with a clear error message.
//
// When enabled, arrow references (e.g., "parent->view") will automatically
// traverse into their target permissions before visiting the arrow reference itself.
// This enables single-pass validation of cross-definition references.
//
// Example:
//
//	schema, _ := BuildSchemaFromCompiledSchema(compiled)
//	resolved, _ := ResolveSchema(schema)  // Required!
//	opts := NewWalkOptions().
//	    WithStrategy(WalkPostOrder).
//	    WithTraverseArrowTargets(resolved.Schema())
func (opts WalkOptions) WithTraverseArrowTargets(schema *Schema) WalkOptions {
	opts.traverseArrowTargets = true
	opts.schema = schema
	return opts
}

// Visitor is the base interface that all specific visitor interfaces embed.
type Visitor[T any] any

// SchemaVisitor is called when visiting a Schema.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type SchemaVisitor[T any] interface {
	VisitSchema(s *Schema, value T) (T, bool, error)
}

// DefinitionVisitor is called when visiting a Definition.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type DefinitionVisitor[T any] interface {
	VisitDefinition(d *Definition, value T) (T, bool, error)
}

// CaveatVisitor is called when visiting a Caveat.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type CaveatVisitor[T any] interface {
	VisitCaveat(c *Caveat, value T) (T, error)
}

// RelationVisitor is called when visiting a Relation.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type RelationVisitor[T any] interface {
	VisitRelation(r *Relation, value T) (T, bool, error)
}

// BaseRelationVisitor is called when visiting a BaseRelation.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type BaseRelationVisitor[T any] interface {
	VisitBaseRelation(br *BaseRelation, value T) (T, error)
}

// PermissionVisitor is called when visiting a Permission.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type PermissionVisitor[T any] interface {
	VisitPermission(p *Permission, value T) (T, bool, error)
}

// OperationVisitor is called when visiting any Operation.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type OperationVisitor[T any] interface {
	VisitOperation(op Operation, value T) (T, bool, error)
}

// RelationReferenceVisitor is called when visiting a RelationReference.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type RelationReferenceVisitor[T any] interface {
	VisitRelationReference(rr *RelationReference, value T) (T, error)
}

// NilReferenceVisitor is called when visiting a NilReference.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type NilReferenceVisitor[T any] interface {
	VisitNilReference(nr *NilReference, value T) (T, error)
}

// ArrowReferenceVisitor is called when visiting an ArrowReference.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type ArrowReferenceVisitor[T any] interface {
	VisitArrowReference(ar *ArrowReference, value T) (T, error)
}

// UnionOperationVisitor is called when visiting a UnionOperation.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type UnionOperationVisitor[T any] interface {
	VisitUnionOperation(uo *UnionOperation, value T) (T, bool, error)
}

// IntersectionOperationVisitor is called when visiting an IntersectionOperation.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type IntersectionOperationVisitor[T any] interface {
	VisitIntersectionOperation(io *IntersectionOperation, value T) (T, bool, error)
}

// ExclusionOperationVisitor is called when visiting an ExclusionOperation.
// Returns the value to pass to subsequent visits, true to continue walking, false to stop, and error to halt immediately.
type ExclusionOperationVisitor[T any] interface {
	VisitExclusionOperation(eo *ExclusionOperation, value T) (T, bool, error)
}

// ResolvedRelationReferenceVisitor is called when visiting a ResolvedRelationReference.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type ResolvedRelationReferenceVisitor[T any] interface {
	VisitResolvedRelationReference(rrr *ResolvedRelationReference, value T) (T, error)
}

// ResolvedArrowReferenceVisitor is called when visiting a ResolvedArrowReference.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type ResolvedArrowReferenceVisitor[T any] interface {
	VisitResolvedArrowReference(rar *ResolvedArrowReference, value T) (T, error)
}

// ResolvedFunctionedArrowReferenceVisitor is called when visiting a ResolvedFunctionedArrowReference.
// Returns the value to pass to subsequent visits, and error to halt immediately.
type ResolvedFunctionedArrowReferenceVisitor[T any] interface {
	VisitResolvedFunctionedArrowReference(rfar *ResolvedFunctionedArrowReference, value T) (T, error)
}

// ArrowOperationVisitor is called when visiting any ArrowOperation (ArrowReference, FunctionedArrowReference, ResolvedArrowReference, or ResolvedFunctionedArrowReference).
// Returns the value to pass to subsequent visits, and error to halt immediately.
type ArrowOperationVisitor[T any] interface {
	VisitArrowOperation(ao ArrowOperation, value T) (T, error)
}

// WalkSchema walks the entire schema tree, calling appropriate visitor methods
// on the provided Visitor for each node encountered. Returns the final value and error if any visitor returns an error.
func WalkSchema[T any](s *Schema, v Visitor[T], value T) (T, error) {
	return walkSchemaWithOptions(s, v, value, NewWalkOptions())
}

// WalkDefinition walks a definition and its relations and permissions.
// Returns the final value and error if any visitor returns an error.
func WalkDefinition[T any](d *Definition, v Visitor[T], value T) (T, error) {
	return walkDefinitionWithOptions(d, v, value, NewWalkOptions())
}

// WalkCaveat walks a caveat. Returns the final value and error if any visitor returns an error.
func WalkCaveat[T any](c *Caveat, v Visitor[T], value T) (T, error) {
	return walkCaveatWithOptions(c, v, value, NewWalkOptions())
}

// WalkRelation walks a relation and its base relations.
// Returns the final value and error if any visitor returns an error.
func WalkRelation[T any](r *Relation, v Visitor[T], value T) (T, error) {
	return walkRelationWithOptions(r, v, value, NewWalkOptions())
}

// WalkBaseRelation walks a base relation. Returns the final value and error if any visitor returns an error.
func WalkBaseRelation[T any](br *BaseRelation, v Visitor[T], value T) (T, error) {
	return walkBaseRelationWithOptions(br, v, value, NewWalkOptions())
}

// WalkPermission walks a permission and its operation tree.
// Returns the final value and error if any visitor returns an error.
func WalkPermission[T any](p *Permission, v Visitor[T], value T) (T, error) {
	return walkPermissionWithOptions(p, v, value, NewWalkOptions())
}

// WalkOperation walks an operation tree recursively.
// Returns the final value and error if any visitor returns an error.
func WalkOperation[T any](op Operation, v Visitor[T], value T) (T, error) {
	return walkOperationWithOptions(op, v, value, NewWalkOptions())
}

// walkSchemaWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
func walkSchemaWithOptions[T any](s *Schema, v Visitor[T], value T, options WalkOptions) (T, error) {
	if s == nil {
		return value, nil
	}

	// Validate: arrow traversal requires PostOrder strategy
	if options.traverseArrowTargets && options.strategy != WalkPostOrder {
		return value, errors.New("TraverseArrowTargets requires PostOrder strategy")
	}

	currentValue := value

	if options.strategy == WalkPostOrder {
		// PostOrder: Visit children first, then parent
		for _, def := range s.definitions {
			newValue, err := walkDefinitionWithOptions(def, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		for _, caveat := range s.caveats {
			newValue, err := walkCaveatWithOptions(caveat, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		// Visit schema after children
		if sv, ok := v.(SchemaVisitor[T]); ok {
			newValue, _, err := sv.VisitSchema(s, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		return currentValue, nil
	}

	// PreOrder: Visit parent first, then children (current behavior)
	shouldVisitChildren := true
	if sv, ok := v.(SchemaVisitor[T]); ok {
		newValue, cont, err := sv.VisitSchema(s, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		shouldVisitChildren = cont
	}

	if shouldVisitChildren {
		for _, def := range s.definitions {
			newValue, err := walkDefinitionWithOptions(def, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		for _, caveat := range s.caveats {
			newValue, err := walkCaveatWithOptions(caveat, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}
	}

	return currentValue, nil
}

// walkDefinitionWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
func walkDefinitionWithOptions[T any](d *Definition, v Visitor[T], value T, options WalkOptions) (T, error) {
	if d == nil {
		return value, nil
	}

	currentValue := value

	if options.strategy == WalkPostOrder {
		// PostOrder: Visit children first, then parent
		for _, rel := range d.relations {
			newValue, err := walkRelationWithOptions(rel, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		for _, perm := range d.permissions {
			newValue, err := walkPermissionWithOptions(perm, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		// Visit definition after children
		if dv, ok := v.(DefinitionVisitor[T]); ok {
			newValue, _, err := dv.VisitDefinition(d, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		return currentValue, nil
	}

	// PreOrder: Visit parent first, then children
	shouldVisitChildren := true
	if dv, ok := v.(DefinitionVisitor[T]); ok {
		newValue, cont, err := dv.VisitDefinition(d, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		shouldVisitChildren = cont
	}

	if shouldVisitChildren {
		for _, rel := range d.relations {
			newValue, err := walkRelationWithOptions(rel, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		for _, perm := range d.permissions {
			newValue, err := walkPermissionWithOptions(perm, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}
	}

	return currentValue, nil
}

// walkCaveatWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
// Since Caveat is a terminal node (no children), the strategy doesn't affect behavior.
func walkCaveatWithOptions[T any](c *Caveat, v Visitor[T], value T, options WalkOptions) (T, error) {
	if c == nil {
		return value, nil
	}

	if cv, ok := v.(CaveatVisitor[T]); ok {
		newValue, err := cv.VisitCaveat(c, value)
		if err != nil {
			return value, err
		}
		return newValue, nil
	}

	return value, nil
}

// walkRelationWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
func walkRelationWithOptions[T any](r *Relation, v Visitor[T], value T, options WalkOptions) (T, error) {
	if r == nil {
		return value, nil
	}

	currentValue := value

	if options.strategy == WalkPostOrder {
		// PostOrder: Visit children first, then parent
		for _, br := range r.baseRelations {
			newValue, err := walkBaseRelationWithOptions(br, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		// Visit relation after children
		if rv, ok := v.(RelationVisitor[T]); ok {
			newValue, _, err := rv.VisitRelation(r, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		return currentValue, nil
	}

	// PreOrder: Visit parent first, then children
	shouldVisitChildren := true
	if rv, ok := v.(RelationVisitor[T]); ok {
		newValue, cont, err := rv.VisitRelation(r, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		shouldVisitChildren = cont
	}

	if shouldVisitChildren {
		for _, br := range r.baseRelations {
			newValue, err := walkBaseRelationWithOptions(br, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}
	}

	return currentValue, nil
}

// walkBaseRelationWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
// Since BaseRelation is a terminal node (no children), the strategy doesn't affect behavior.
func walkBaseRelationWithOptions[T any](br *BaseRelation, v Visitor[T], value T, options WalkOptions) (T, error) {
	if br == nil {
		return value, nil
	}

	if brv, ok := v.(BaseRelationVisitor[T]); ok {
		newValue, err := brv.VisitBaseRelation(br, value)
		if err != nil {
			return value, err
		}
		return newValue, nil
	}

	return value, nil
}

// walkPermissionWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
func walkPermissionWithOptions[T any](p *Permission, v Visitor[T], value T, options WalkOptions) (T, error) {
	if p == nil {
		return value, nil
	}

	currentValue := value

	if options.strategy == WalkPostOrder {
		// PostOrder: Visit children (operation tree) first, then parent
		newValue, err := walkOperationWithOptions(p.operation, v, currentValue, options)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue

		// Visit permission after children
		if pv, ok := v.(PermissionVisitor[T]); ok {
			newValue, _, err := pv.VisitPermission(p, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		return currentValue, nil
	}

	// PreOrder: Visit parent first, then children
	shouldVisitChildren := true
	if pv, ok := v.(PermissionVisitor[T]); ok {
		newValue, cont, err := pv.VisitPermission(p, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		shouldVisitChildren = cont
	}

	if shouldVisitChildren {
		return walkOperationWithOptions(p.operation, v, currentValue, options)
	}

	return currentValue, nil
}

// walkOperationWithOptions is the internal implementation that supports both PreOrder and PostOrder strategies.
func walkOperationWithOptions[T any](op Operation, v Visitor[T], value T, options WalkOptions) (T, error) {
	if op == nil {
		return value, nil
	}

	// Validate: arrow traversal requires PostOrder strategy
	if options.traverseArrowTargets && options.strategy != WalkPostOrder {
		return value, errors.New("TraverseArrowTargets requires PostOrder strategy")
	}

	// If arrow traversal is enabled, ensure visited set is available.
	// This is the central initialization point for all operation walks to avoid duplication.
	if options.traverseArrowTargets {
		if options.visitedArrowTargets == nil {
			options.visitedArrowTargets = make(map[string]bool)
		}
	}

	if options.strategy == WalkPostOrder {
		return walkOperationPostOrder(op, v, value, options)
	}
	return walkOperationPreOrder(op, v, value, options)
}

// walkOperationPostOrder handles PostOrder traversal of operation trees.
func walkOperationPostOrder[T any](op Operation, v Visitor[T], value T, options WalkOptions) (T, error) {
	currentValue := value

	// PostOrder: Visit children first, then parent
	switch o := op.(type) {
	case *RelationReference:
		// Terminal node - visit immediately
		if rrv, ok := v.(RelationReferenceVisitor[T]); ok {
			newValue, err := rrv.VisitRelationReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *NilReference:
		// Terminal node - visit immediately
		if nrv, ok := v.(NilReferenceVisitor[T]); ok {
			newValue, err := nrv.VisitNilReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ArrowReference:
		// If arrow traversal is enabled but schema is not resolved, provide helpful error
		if options.traverseArrowTargets {
			return currentValue, errors.New(
				"TraverseArrowTargets requires a resolved schema. " +
					"Call ResolveSchema() on your schema before walking with arrow traversal enabled. " +
					"Unresolved ArrowReference found: " + o.Left() + "->" + o.Right())
		}

		// Terminal node - call visitors
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if arv, ok := v.(ArrowReferenceVisitor[T]); ok {
			newValue, err := arv.VisitArrowReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *FunctionedArrowReference:
		// Terminal node - call visitor
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ResolvedRelationReference:
		// walk resolved relation first
		switch resolved := o.Resolved().(type) {
		case *Relation:
			newValue, err := walkRelationWithOptions(resolved, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		case *Permission:
			newValue, err := walkPermissionWithOptions(resolved, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if rrrv, ok := v.(ResolvedRelationReferenceVisitor[T]); ok {
			newValue, err := rrrv.VisitResolvedRelationReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ResolvedArrowReference:
		// If arrow traversal is enabled, walk target permissions first (PostOrder)
		if options.traverseArrowTargets {
			// Schema is required for arrow traversal
			if options.schema == nil {
				return currentValue, errors.New(
					"TraverseArrowTargets requires a schema for target resolution. " +
						"Arrow reference: " + o.Left() + "->" + o.Right())
			}

			// Get the resolved left relation to find allowed subject types
			resolvedLeft := o.ResolvedLeft()
			if resolvedLeft != nil {
				// For each base relation (allowed subject type), walk the target permission
				for _, br := range resolvedLeft.BaseRelations() {
					targetDefName := br.Type()
					targetPermName := o.Right()
					targetKey := targetDefName + "#" + targetPermName

					// Skip if already visited to prevent infinite recursion
					if options.visitedArrowTargets[targetKey] {
						continue
					}
					options.visitedArrowTargets[targetKey] = true

					// Find the target definition in the schema
					if targetDef, ok := options.schema.GetTypeDefinition(targetDefName); ok {
						// Find the target relation/permission and walk it
						if targetRel, ok := targetDef.GetRelation(targetPermName); ok {
							newValue, err := walkRelationWithOptions(targetRel, v, currentValue, options)
							if err != nil {
								return currentValue, err
							}
							currentValue = newValue
						} else if targetPerm, ok := targetDef.GetPermission(targetPermName); ok {
							newValue, err := walkPermissionWithOptions(targetPerm, v, currentValue, options)
							if err != nil {
								return currentValue, err
							}
							currentValue = newValue
						}
					}
				}
			}
		}

		// Call visitors after traversing targets (in PostOrder)
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if rarv, ok := v.(ResolvedArrowReferenceVisitor[T]); ok {
			newValue, err := rarv.VisitResolvedArrowReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ResolvedFunctionedArrowReference:
		// If arrow traversal is enabled, walk target permissions first (PostOrder)
		if options.traverseArrowTargets {
			// Schema is required for arrow traversal
			if options.schema == nil {
				return currentValue, errors.New(
					"TraverseArrowTargets requires a schema for target resolution. " +
						"Functioned arrow reference: " + o.Left() + "->" + o.Right())
			}

			// Get the resolved left relation to find allowed subject types
			resolvedLeft := o.ResolvedLeft()
			if resolvedLeft != nil {
				// For each base relation (allowed subject type), walk the target permission
				for _, br := range resolvedLeft.BaseRelations() {
					targetDefName := br.Type()
					targetPermName := o.Right()
					targetKey := targetDefName + "#" + targetPermName

					// Skip if already visited to prevent infinite recursion
					if options.visitedArrowTargets[targetKey] {
						continue
					}
					options.visitedArrowTargets[targetKey] = true

					// Find the target definition in the schema
					if targetDef, ok := options.schema.GetTypeDefinition(targetDefName); ok {
						// Find the target relation/permission and walk it
						if targetRel, ok := targetDef.GetRelation(targetPermName); ok {
							newValue, err := walkRelationWithOptions(targetRel, v, currentValue, options)
							if err != nil {
								return currentValue, err
							}
							currentValue = newValue
						} else if targetPerm, ok := targetDef.GetPermission(targetPermName); ok {
							newValue, err := walkPermissionWithOptions(targetPerm, v, currentValue, options)
							if err != nil {
								return currentValue, err
							}
							currentValue = newValue
						}
					}
				}
			}
		}

		// Call visitors after traversing targets (in PostOrder)
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if rfarv, ok := v.(ResolvedFunctionedArrowReferenceVisitor[T]); ok {
			newValue, err := rfarv.VisitResolvedFunctionedArrowReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *UnionOperation:
		// Visit children first
		for _, child := range o.children {
			newValue, err := walkOperationWithOptions(child, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		// Then visit parent
		if ov, ok := v.(OperationVisitor[T]); ok {
			newValue, _, err := ov.VisitOperation(op, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if uov, ok := v.(UnionOperationVisitor[T]); ok {
			newValue, _, err := uov.VisitUnionOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *IntersectionOperation:
		// Visit children first
		for _, child := range o.children {
			newValue, err := walkOperationWithOptions(child, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		// Then visit parent
		if ov, ok := v.(OperationVisitor[T]); ok {
			newValue, _, err := ov.VisitOperation(op, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if iov, ok := v.(IntersectionOperationVisitor[T]); ok {
			newValue, _, err := iov.VisitIntersectionOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ExclusionOperation:
		// Visit children first
		newValue, err := walkOperationWithOptions(o.left, v, currentValue, options)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue

		newValue, err = walkOperationWithOptions(o.right, v, currentValue, options)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue

		// Then visit parent
		if ov, ok := v.(OperationVisitor[T]); ok {
			newValue, _, err := ov.VisitOperation(op, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if eov, ok := v.(ExclusionOperationVisitor[T]); ok {
			newValue, _, err := eov.VisitExclusionOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}
	}

	return currentValue, nil
}

// walkOperationPreOrder handles PreOrder traversal of operation trees.
func walkOperationPreOrder[T any](op Operation, v Visitor[T], value T, options WalkOptions) (T, error) {
	currentValue := value

	// PreOrder: Visit parent first, then children
	shouldVisitChildren := true
	if ov, ok := v.(OperationVisitor[T]); ok {
		newValue, cont, err := ov.VisitOperation(op, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		shouldVisitChildren = cont
	}

	if !shouldVisitChildren {
		return currentValue, nil
	}

	switch o := op.(type) {
	case *RelationReference:
		if rrv, ok := v.(RelationReferenceVisitor[T]); ok {
			newValue, err := rrv.VisitRelationReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *NilReference:
		if nrv, ok := v.(NilReferenceVisitor[T]); ok {
			newValue, err := nrv.VisitNilReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ArrowReference:
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if arv, ok := v.(ArrowReferenceVisitor[T]); ok {
			newValue, err := arv.VisitArrowReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *FunctionedArrowReference:
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ResolvedRelationReference:
		if rrrv, ok := v.(ResolvedRelationReferenceVisitor[T]); ok {
			newValue, err := rrrv.VisitResolvedRelationReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ResolvedArrowReference:
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if rarv, ok := v.(ResolvedArrowReferenceVisitor[T]); ok {
			newValue, err := rarv.VisitResolvedArrowReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ResolvedFunctionedArrowReference:
		if aov, ok := v.(ArrowOperationVisitor[T]); ok {
			newValue, err := aov.VisitArrowOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

		if rfarv, ok := v.(ResolvedFunctionedArrowReferenceVisitor[T]); ok {
			newValue, err := rfarv.VisitResolvedFunctionedArrowReference(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *UnionOperation:
		if uov, ok := v.(UnionOperationVisitor[T]); ok {
			newValue, cont, err := uov.VisitUnionOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
			if !cont {
				return currentValue, nil
			}
		}
		for _, child := range o.children {
			newValue, err := walkOperationWithOptions(child, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *IntersectionOperation:
		if iov, ok := v.(IntersectionOperationVisitor[T]); ok {
			newValue, cont, err := iov.VisitIntersectionOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
			if !cont {
				return currentValue, nil
			}
		}
		for _, child := range o.children {
			newValue, err := walkOperationWithOptions(child, v, currentValue, options)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
		}

	case *ExclusionOperation:
		if eov, ok := v.(ExclusionOperationVisitor[T]); ok {
			newValue, cont, err := eov.VisitExclusionOperation(o, currentValue)
			if err != nil {
				return currentValue, err
			}
			currentValue = newValue
			if !cont {
				return currentValue, nil
			}
		}
		newValue, err := walkOperationWithOptions(o.left, v, currentValue, options)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue

		newValue, err = walkOperationWithOptions(o.right, v, currentValue, options)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	return currentValue, nil
}

// WalkSchemaWithOptions walks the entire schema tree with custom traversal options, calling appropriate visitor methods
// on the provided Visitor for each node encountered. Returns the final value and error if any visitor returns an error.
//
// The strategy parameter controls traversal order:
//   - WalkPreOrder (default): Visits each node before its children
//   - WalkPostOrder: Visits each node after its children
//
// In pre-order traversal, visitor methods returning cont=false will skip that node's children.
// In post-order traversal, children are visited before the parent, so cont=false only affects
// the current level's continuation.
func WalkSchemaWithOptions[T any](s *Schema, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkSchemaWithOptions(s, v, value, options)
}

// WalkDefinitionWithOptions walks a definition and its relations and permissions with custom traversal options.
// Returns the final value and error if any visitor returns an error.
func WalkDefinitionWithOptions[T any](d *Definition, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkDefinitionWithOptions(d, v, value, options)
}

// WalkCaveatWithOptions walks a caveat with custom traversal options.
// Returns the final value and error if any visitor returns an error.
func WalkCaveatWithOptions[T any](c *Caveat, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkCaveatWithOptions(c, v, value, options)
}

// WalkRelationWithOptions walks a relation and its base relations with custom traversal options.
// Returns the final value and error if any visitor returns an error.
func WalkRelationWithOptions[T any](r *Relation, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkRelationWithOptions(r, v, value, options)
}

// WalkBaseRelationWithOptions walks a base relation with custom traversal options.
// Returns the final value and error if any visitor returns an error.
func WalkBaseRelationWithOptions[T any](br *BaseRelation, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkBaseRelationWithOptions(br, v, value, options)
}

// WalkPermissionWithOptions walks a permission and its operation tree with custom traversal options.
// Returns the final value and error if any visitor returns an error.
func WalkPermissionWithOptions[T any](p *Permission, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkPermissionWithOptions(p, v, value, options)
}

// WalkOperationWithOptions walks an operation tree recursively with custom traversal options.
// Returns the final value and error if any visitor returns an error.
func WalkOperationWithOptions[T any](op Operation, v Visitor[T], value T, options WalkOptions) (T, error) {
	return walkOperationWithOptions(op, v, value, options)
}
