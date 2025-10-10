package schema

// Operation is a closed enum of things that can exist on the right-hand-side of a permission.
// It forms a tree of unions, intersections and exclusions, until the leaves are things like references to other permissions or relations, or are arrows.
type Operation interface {
	isOperation()
	clone() Operation
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

var _ schemaUnit[Operation] = &ExclusionOperation{}
var _ schemaUnit[Operation] = &ResolvedRelationReference{}
var _ schemaUnit[Operation] = &ResolvedArrowReference{}

// FunctionedTuplesetOperation is an Operation that represents functioned tuplesets like `permission foo = relation.any(other)` or `permission foo = relation.all(other)`.
type FunctionedTuplesetOperation struct {
	tuplesetRelation string
	function         FunctionType
	computedRelation string
}

// FunctionType represents the type of function applied to a tupleset.
type FunctionType int

const (
	FunctionTypeAny FunctionType = iota
	FunctionTypeAll
)

func (f *FunctionedTuplesetOperation) TuplesetRelation() string {
	return f.tuplesetRelation
}

func (f *FunctionedTuplesetOperation) Function() FunctionType {
	return f.function
}

func (f *FunctionedTuplesetOperation) ComputedRelation() string {
	return f.computedRelation
}

// clone creates a deep copy of the FunctionedTuplesetOperation.
func (f *FunctionedTuplesetOperation) clone() Operation {
	if f == nil {
		return nil
	}
	return &FunctionedTuplesetOperation{
		tuplesetRelation: f.tuplesetRelation,
		function:         f.function,
		computedRelation: f.computedRelation,
	}
}

// We close the enum by implementing the private method.
func (r *RelationReference) isOperation()             {}
func (a *ArrowReference) isOperation()                {}
func (u *UnionOperation) isOperation()                {}
func (i *IntersectionOperation) isOperation()         {}
func (e *ExclusionOperation) isOperation()            {}
func (f *FunctionedTuplesetOperation) isOperation()   {}
func (r *ResolvedRelationReference) isOperation()     {}
func (a *ResolvedArrowReference) isOperation()        {}

var (
	_ Operation = (*RelationReference)(nil)
	_ Operation = (*ArrowReference)(nil)
	_ Operation = (*UnionOperation)(nil)
	_ Operation = (*IntersectionOperation)(nil)
	_ Operation = (*ExclusionOperation)(nil)
	_ Operation = (*FunctionedTuplesetOperation)(nil)
	_ Operation = (*ResolvedRelationReference)(nil)
	_ Operation = (*ResolvedArrowReference)(nil)
)
