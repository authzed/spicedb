package schema

// Operation is a closed enum of things that can exist on the right-hand-side of a permission.
// It forms a tree of unions, intersections and exclusions, until the leaves are things like references to other permissions or relations, or are arrows.
type Operation interface {
	isOperation()
	clone() Operation
}

// ArrowOperation is an interface implemented by all arrow-based operations (both standard and functioned arrows).
// This includes ArrowReference, FunctionedArrowReference, ResolvedArrowReference, and ResolvedFunctionedArrowReference.
type ArrowOperation interface {
	Operation

	// Left returns the relation on the resource (left side of the arrow).
	Left() string

	// Right returns the relation/permission on the subject (right side of the arrow).
	Right() string

	// Function returns the function type applied to the arrow.
	// For standard arrows (->), this returns FunctionTypeAny.
	// For functioned arrows (.any()/.all()), this returns the specific function type.
	Function() FunctionType
}

// RelationReference is an Operation that is a simple relation, such as `permission foo = bar`.
type RelationReference struct {
	// relationName is the name of the relation or permission being referenced.
	relationName string
}

// RelationName returns the name of the relation or permission being referenced.
func (r *RelationReference) RelationName() string {
	return r.relationName
}

// clone creates a deep copy of the RelationReference.
func (r *RelationReference) clone() Operation {
	if r == nil {
		return nil
	}
	return &RelationReference{
		relationName: r.relationName,
	}
}

var _ schemaUnit[Operation] = &RelationReference{}

// NilReference is a specialized Operation that represents a nil/empty operation.
// Unlike RelationReference, it is not replaced during resolution.
type NilReference struct{}

// clone creates a copy of the NilReference.
func (n *NilReference) clone() Operation {
	if n == nil {
		return nil
	}
	return &NilReference{}
}

var _ schemaUnit[Operation] = &NilReference{}

// ArrowReference is an Operation that represents `permission foo = Left->Right`.
type ArrowReference struct {
	// left is the relation on the resource.
	left string

	// right is the relation/permission on the subject.
	right string
}

// Left returns the relation on the resource.
func (a *ArrowReference) Left() string {
	return a.left
}

// Right returns the relation/permission on the subject.
func (a *ArrowReference) Right() string {
	return a.right
}

// Function returns FunctionTypeAny for standard arrows.
func (a *ArrowReference) Function() FunctionType {
	return FunctionTypeAny
}

// clone creates a deep copy of the ArrowReference.
func (a *ArrowReference) clone() Operation {
	if a == nil {
		return nil
	}
	return &ArrowReference{
		left:  a.left,
		right: a.right,
	}
}

var _ schemaUnit[Operation] = &ArrowReference{}

// UnionOperation is an Operation that represents `permission foo = a | b | c`.
type UnionOperation struct {
	// children are the sub-operations that are unioned together.
	children []Operation
}

// Children returns the sub-operations that are unioned together.
func (u *UnionOperation) Children() []Operation {
	return u.children
}

// clone creates a deep copy of the UnionOperation.
func (u *UnionOperation) clone() Operation {
	if u == nil {
		return nil
	}
	children := make([]Operation, len(u.children))
	for i, child := range u.children {
		children[i] = child.clone()
	}
	return &UnionOperation{
		children: children,
	}
}

var _ schemaUnit[Operation] = &UnionOperation{}

// IntersectionOperation is an Operation that represents `permission foo = a & b & c`.
type IntersectionOperation struct {
	children []Operation
}

// Children returns the sub-operations that are intersected together.
func (i *IntersectionOperation) Children() []Operation {
	return i.children
}

// clone creates a deep copy of the IntersectionOperation.
func (i *IntersectionOperation) clone() Operation {
	if i == nil {
		return nil
	}
	children := make([]Operation, len(i.children))
	for idx, child := range i.children {
		children[idx] = child.clone()
	}
	return &IntersectionOperation{
		children: children,
	}
}

var _ schemaUnit[Operation] = &IntersectionOperation{}

// ExclusionOperation is an Operation that represents `permission foo = a - b`.
type ExclusionOperation struct {
	// left is the operation from which we are excluding.
	left Operation

	// right is the operation that is being excluded.
	right Operation
}

// ResolvedRelationReference is an Operation that is a resolved relation reference.
// It contains both the name and the actual RelationOrPermission being referenced.
type ResolvedRelationReference struct {
	// relationName is the name of the relation or permission being referenced.
	relationName string

	// resolved is the actual RelationOrPermission being referenced.
	resolved RelationOrPermission
}

// ResolvedArrowReference is an Operation that represents a resolved arrow reference.
// It contains the resolved left side relation and the name of the right side.
type ResolvedArrowReference struct {
	// left is the name of the relation on the resource.
	left string

	// resolvedLeft is the actual Relation being referenced on the left side.
	resolvedLeft *Relation

	// right is the name of the relation/permission on the subject.
	right string
}

// Left returns the operation from which we are excluding.
func (e *ExclusionOperation) Left() Operation {
	return e.left
}

// Right returns the operation that is being excluded.
func (e *ExclusionOperation) Right() Operation {
	return e.right
}

// clone creates a deep copy of the ExclusionOperation.
func (e *ExclusionOperation) clone() Operation {
	if e == nil {
		return nil
	}
	return &ExclusionOperation{
		left:  e.left.clone(),
		right: e.right.clone(),
	}
}

// RelationName returns the name of the relation or permission being referenced.
func (r *ResolvedRelationReference) RelationName() string {
	return r.relationName
}

// Resolved returns the actual RelationOrPermission being referenced.
func (r *ResolvedRelationReference) Resolved() RelationOrPermission {
	return r.resolved
}

// clone creates a deep copy of the ResolvedRelationReference.
func (r *ResolvedRelationReference) clone() Operation {
	if r == nil {
		return nil
	}
	return &ResolvedRelationReference{
		relationName: r.relationName,
		resolved:     r.resolved,
	}
}

// Left returns the name of the relation on the resource.
func (a *ResolvedArrowReference) Left() string {
	return a.left
}

// ResolvedLeft returns the actual Relation being referenced on the left side.
func (a *ResolvedArrowReference) ResolvedLeft() *Relation {
	return a.resolvedLeft
}

// Right returns the name of the relation/permission on the subject.
func (a *ResolvedArrowReference) Right() string {
	return a.right
}

// Function returns FunctionTypeAny for standard resolved arrows.
func (a *ResolvedArrowReference) Function() FunctionType {
	return FunctionTypeAny
}

// clone creates a deep copy of the ResolvedArrowReference.
func (a *ResolvedArrowReference) clone() Operation {
	if a == nil {
		return nil
	}
	return &ResolvedArrowReference{
		left:         a.left,
		resolvedLeft: a.resolvedLeft,
		right:        a.right,
	}
}

// ResolvedFunctionedArrowReference is an Operation that represents a resolved functioned arrow reference.
// It contains the resolved left side relation, the name of the right side, and the function type.
type ResolvedFunctionedArrowReference struct {
	// left is the name of the relation on the resource.
	left string

	// resolvedLeft is the actual Relation being referenced on the left side.
	resolvedLeft *Relation

	// right is the name of the relation/permission on the subject.
	right string

	// function is the function type (any or all).
	function FunctionType
}

// Left returns the name of the relation on the resource.
func (a *ResolvedFunctionedArrowReference) Left() string {
	return a.left
}

// ResolvedLeft returns the actual Relation being referenced on the left side.
func (a *ResolvedFunctionedArrowReference) ResolvedLeft() *Relation {
	return a.resolvedLeft
}

// Right returns the name of the relation/permission on the subject.
func (a *ResolvedFunctionedArrowReference) Right() string {
	return a.right
}

// Function returns the function type.
func (a *ResolvedFunctionedArrowReference) Function() FunctionType {
	return a.function
}

// clone creates a deep copy of the ResolvedFunctionedArrowReference.
func (a *ResolvedFunctionedArrowReference) clone() Operation {
	if a == nil {
		return nil
	}
	return &ResolvedFunctionedArrowReference{
		left:         a.left,
		resolvedLeft: a.resolvedLeft,
		right:        a.right,
		function:     a.function,
	}
}

var (
	_ schemaUnit[Operation] = &ExclusionOperation{}
	_ schemaUnit[Operation] = &ResolvedRelationReference{}
	_ schemaUnit[Operation] = &ResolvedArrowReference{}
	_ schemaUnit[Operation] = &ResolvedFunctionedArrowReference{}
)

// FunctionedArrowReference is an Operation that represents functioned arrows like `permission foo = relation.any(other)` or `permission foo = relation.all(other)`.
type FunctionedArrowReference struct {
	left     string
	right    string
	function FunctionType
}

// FunctionType represents the type of function applied to a tupleset.
type FunctionType int

const (
	FunctionTypeAny FunctionType = iota
	FunctionTypeAll
)

func (f *FunctionedArrowReference) Left() string {
	return f.left
}

func (f *FunctionedArrowReference) Right() string {
	return f.right
}

func (f *FunctionedArrowReference) Function() FunctionType {
	return f.function
}

// clone creates a deep copy of the FunctionedArrowReference.
func (f *FunctionedArrowReference) clone() Operation {
	if f == nil {
		return nil
	}
	return &FunctionedArrowReference{
		left:     f.left,
		right:    f.right,
		function: f.function,
	}
}

// We close the enum by implementing the private method.
func (r *RelationReference) isOperation()                {}
func (n *NilReference) isOperation()                     {}
func (a *ArrowReference) isOperation()                   {}
func (u *UnionOperation) isOperation()                   {}
func (i *IntersectionOperation) isOperation()            {}
func (e *ExclusionOperation) isOperation()               {}
func (f *FunctionedArrowReference) isOperation()         {}
func (r *ResolvedRelationReference) isOperation()        {}
func (a *ResolvedArrowReference) isOperation()           {}
func (a *ResolvedFunctionedArrowReference) isOperation() {}

var (
	_ Operation = (*RelationReference)(nil)
	_ Operation = (*NilReference)(nil)
	_ Operation = (*ArrowReference)(nil)
	_ Operation = (*UnionOperation)(nil)
	_ Operation = (*IntersectionOperation)(nil)
	_ Operation = (*ExclusionOperation)(nil)
	_ Operation = (*FunctionedArrowReference)(nil)
	_ Operation = (*ResolvedRelationReference)(nil)
	_ Operation = (*ResolvedArrowReference)(nil)
	_ Operation = (*ResolvedFunctionedArrowReference)(nil)
)
