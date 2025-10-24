package schema

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
	if s == nil {
		return value, nil
	}

	currentValue := value
	if sv, ok := v.(SchemaVisitor[T]); ok {
		newValue, cont, err := sv.VisitSchema(s, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		if !cont {
			return currentValue, nil
		}
	}

	for _, def := range s.definitions {
		newValue, err := WalkDefinition(def, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	for _, caveat := range s.caveats {
		newValue, err := WalkCaveat(caveat, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	return currentValue, nil
}

// WalkDefinition walks a definition and its relations and permissions.
// Returns the final value and error if any visitor returns an error.
func WalkDefinition[T any](d *Definition, v Visitor[T], value T) (T, error) {
	if d == nil {
		return value, nil
	}

	currentValue := value
	if dv, ok := v.(DefinitionVisitor[T]); ok {
		newValue, cont, err := dv.VisitDefinition(d, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		if !cont {
			return currentValue, nil
		}
	}

	for _, rel := range d.relations {
		newValue, err := WalkRelation(rel, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	for _, perm := range d.permissions {
		newValue, err := WalkPermission(perm, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	return currentValue, nil
}

// WalkCaveat walks a caveat. Returns the final value and error if any visitor returns an error.
func WalkCaveat[T any](c *Caveat, v Visitor[T], value T) (T, error) {
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

// WalkRelation walks a relation and its base relations.
// Returns the final value and error if any visitor returns an error.
func WalkRelation[T any](r *Relation, v Visitor[T], value T) (T, error) {
	if r == nil {
		return value, nil
	}

	currentValue := value
	if rv, ok := v.(RelationVisitor[T]); ok {
		newValue, cont, err := rv.VisitRelation(r, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		if !cont {
			return currentValue, nil
		}
	}

	for _, br := range r.baseRelations {
		newValue, err := WalkBaseRelation(br, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	return currentValue, nil
}

// WalkBaseRelation walks a base relation. Returns the final value and error if any visitor returns an error.
func WalkBaseRelation[T any](br *BaseRelation, v Visitor[T], value T) (T, error) {
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

// WalkPermission walks a permission and its operation tree.
// Returns the final value and error if any visitor returns an error.
func WalkPermission[T any](p *Permission, v Visitor[T], value T) (T, error) {
	if p == nil {
		return value, nil
	}

	currentValue := value
	if pv, ok := v.(PermissionVisitor[T]); ok {
		newValue, cont, err := pv.VisitPermission(p, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		if !cont {
			return currentValue, nil
		}
	}

	return WalkOperation(p.operation, v, currentValue)
}

// WalkOperation walks an operation tree recursively.
// Returns the final value and error if any visitor returns an error.
func WalkOperation[T any](op Operation, v Visitor[T], value T) (T, error) {
	if op == nil {
		return value, nil
	}

	currentValue := value
	if ov, ok := v.(OperationVisitor[T]); ok {
		newValue, cont, err := ov.VisitOperation(op, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
		if !cont {
			return currentValue, nil
		}
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
		// Call ArrowOperationVisitor if present
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
		// Call ArrowOperationVisitor if present
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
		// Call ArrowOperationVisitor if present
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
		// Call ArrowOperationVisitor if present
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
			newValue, err := WalkOperation(child, v, currentValue)
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
			newValue, err := WalkOperation(child, v, currentValue)
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
		newValue, err := WalkOperation(o.left, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue

		newValue, err = WalkOperation(o.right, v, currentValue)
		if err != nil {
			return currentValue, err
		}
		currentValue = newValue
	}

	return currentValue, nil
}
