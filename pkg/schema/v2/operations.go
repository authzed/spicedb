package schema

// Operation is a closed enum of things that can exist on the right-hand-side of a permission.
// It forms a tree of unions, intersections and exclusions, until the leaves are things like references to other permissions or relations, or are arrows.
type Operation interface {
	isOperation()
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

// UnionOperation is an Operation that represents `permission foo = a | b | c`.
type UnionOperation struct {
	// children are the sub-operations that are unioned together.
	children []Operation
}

// Children returns the sub-operations that are unioned together.
func (u *UnionOperation) Children() []Operation {
	return u.children
}

// IntersectionOperation is an Operation that represents `permission foo = a & b & c`.
type IntersectionOperation struct {
	children []Operation
}

// Children returns the sub-operations that are intersected together.
func (i *IntersectionOperation) Children() []Operation {
	return i.children
}

// ExclusionOperation is an Operation that represents `permission foo = a - b`.
type ExclusionOperation struct {
	// left is the operation from which we are excluding.
	left Operation

	// right is the operation that is being excluded.
	right Operation
}

// Left returns the operation from which we are excluding.
func (e *ExclusionOperation) Left() Operation {
	return e.left
}

// Right returns the operation that is being excluded.
func (e *ExclusionOperation) Right() Operation {
	return e.right
}

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

// We close the enum by implementing the private method.
func (r *RelationReference) isOperation()           {}
func (a *ArrowReference) isOperation()              {}
func (u *UnionOperation) isOperation()              {}
func (i *IntersectionOperation) isOperation()       {}
func (e *ExclusionOperation) isOperation()          {}
func (f *FunctionedTuplesetOperation) isOperation() {}

var (
	_ Operation = (*RelationReference)(nil)
	_ Operation = (*ArrowReference)(nil)
	_ Operation = (*UnionOperation)(nil)
	_ Operation = (*IntersectionOperation)(nil)
	_ Operation = (*ExclusionOperation)(nil)
	_ Operation = (*FunctionedTuplesetOperation)(nil)
)
