package schema

// Operation is a closed enum of things that can exist on the right-hand-side of a permission.
// It forms a tree of unions, intersections and exclusions, until the leaves are things like references to other permissions or relations, or are arrows.
type Operation interface {
	isOperation()
}

// RelationReference is an Operation that is a simple relation, such as `permission foo = bar`.
type RelationReference struct {
	RelationName string
}

// ArrowReference is an Operation that represents `permission foo = Left->Right`.
type ArrowReference struct {
	Left  string
	Right string
}

// UnionOperation is an Operation that represents `permission foo = a | b | c`.
type UnionOperation struct {
	Children []Operation
}

// IntersectionOperation is an Operation that represents `permission foo = a & b & c`.
type IntersectionOperation struct {
	Children []Operation
}

// IntersectionOperation is an Operation that represents `permission foo = a - b`.
type ExclusionOperation struct {
	Left  Operation
	Right Operation
}

// We close the enum by implementing the private method.
func (r *RelationReference) isOperation()     {}
func (r *ArrowReference) isOperation()        {}
func (u *UnionOperation) isOperation()        {}
func (i *IntersectionOperation) isOperation() {}
func (e *ExclusionOperation) isOperation()    {}
