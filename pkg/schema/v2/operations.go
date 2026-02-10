package schema

// Operation is a closed enum of things that can exist on the right-hand-side of a permission.
// It forms a tree of unions, intersections and exclusions, until the leaves are things like references to other permissions or relations, or are arrows.
type Operation interface {
	Parented
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

// operationCloner is an interface for operations that support cloning with a new parent.
// Not all operations implement this method, so it's checked at runtime via type assertion.
type operationCloner interface {
	cloneWithParent(Operation) Operation
}

// RelationReference is an Operation that is a simple relation, such as `permission foo = bar`.
type RelationReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

	// relationName is the name of the relation or permission being referenced.
	relationName string
}

// RelationName returns the name of the relation or permission being referenced.
func (r *RelationReference) RelationName() string {
	return r.relationName
}

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (r *RelationReference) Parent() Parented {
	return r.parent
}

func (r *RelationReference) setParent(p Parented) {
	r.parent = p
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

// cloneWithParent creates a deep copy of the RelationReference with the specified parent.
func (r *RelationReference) cloneWithParent(parent Operation) Operation {
	if r == nil {
		return nil
	}
	return &RelationReference{
		parent:       parent,
		relationName: r.relationName,
	}
}

var _ schemaUnit[Operation] = &RelationReference{}

type SelfReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented
}

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (s *SelfReference) Parent() Parented {
	return s.parent
}

func (s *SelfReference) setParent(p Parented) {
	s.parent = p
}

func (s *SelfReference) clone() Operation {
	if s == nil {
		return nil
	}
	return &SelfReference{}
}

var _ schemaUnit[Operation] = &SelfReference{}

// NilReference is a specialized Operation that represents a nil/empty operation.
// Unlike RelationReference, it is not replaced during resolution.
type NilReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented
}

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (n *NilReference) Parent() Parented {
	return n.parent
}

func (n *NilReference) setParent(p Parented) {
	n.parent = p
}

// clone creates a copy of the NilReference.
func (n *NilReference) clone() Operation {
	if n == nil {
		return nil
	}
	return &NilReference{}
}

// cloneWithParent creates a copy of the NilReference with the specified parent.
func (n *NilReference) cloneWithParent(parent Operation) Operation {
	if n == nil {
		return nil
	}
	return &NilReference{
		parent: parent,
	}
}

var _ schemaUnit[Operation] = &NilReference{}

// ArrowReference is an Operation that represents `permission foo = Left->Right`.
type ArrowReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

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

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (a *ArrowReference) Parent() Parented {
	return a.parent
}

func (a *ArrowReference) setParent(p Parented) {
	a.parent = p
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

// cloneWithParent creates a deep copy of the ArrowReference with the specified parent.
func (a *ArrowReference) cloneWithParent(parent Operation) Operation {
	if a == nil {
		return nil
	}
	return &ArrowReference{
		parent: parent,
		left:   a.left,
		right:  a.right,
	}
}

var _ schemaUnit[Operation] = &ArrowReference{}

// UnionOperation is an Operation that represents `permission foo = a | b | c`.
type UnionOperation struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

	// children are the sub-operations that are unioned together.
	children []Operation
}

// Children returns the sub-operations that are unioned together.
func (u *UnionOperation) Children() []Operation {
	return u.children
}

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (u *UnionOperation) Parent() Parented {
	return u.parent
}

func (u *UnionOperation) setParent(p Parented) {
	u.parent = p
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

// cloneWithParent creates a deep copy of the UnionOperation with the specified parent.
func (u *UnionOperation) cloneWithParent(parent Operation) Operation {
	if u == nil {
		return nil
	}
	cloned := &UnionOperation{
		parent:   parent,
		children: make([]Operation, len(u.children)),
	}
	for i, child := range u.children {
		if childCloner, ok := child.(operationCloner); ok {
			cloned.children[i] = childCloner.cloneWithParent(cloned)
		} else {
			cloned.children[i] = child.clone()
		}
	}
	return cloned
}

var _ schemaUnit[Operation] = &UnionOperation{}

// IntersectionOperation is an Operation that represents `permission foo = a & b & c`.
type IntersectionOperation struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

	children []Operation
}

// Children returns the sub-operations that are intersected together.
func (i *IntersectionOperation) Children() []Operation {
	return i.children
}

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (i *IntersectionOperation) Parent() Parented {
	return i.parent
}

func (i *IntersectionOperation) setParent(p Parented) {
	i.parent = p
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

// cloneWithParent creates a deep copy of the IntersectionOperation with the specified parent.
func (i *IntersectionOperation) cloneWithParent(parent Operation) Operation {
	if i == nil {
		return nil
	}
	cloned := &IntersectionOperation{
		parent:   parent,
		children: make([]Operation, len(i.children)),
	}
	for idx, child := range i.children {
		if childCloner, ok := child.(operationCloner); ok {
			cloned.children[idx] = childCloner.cloneWithParent(cloned)
		} else {
			cloned.children[idx] = child.clone()
		}
	}
	return cloned
}

var _ schemaUnit[Operation] = &IntersectionOperation{}

// ExclusionOperation is an Operation that represents `permission foo = a - b`.
type ExclusionOperation struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

	// left is the operation from which we are excluding.
	left Operation

	// right is the operation that is being excluded.
	right Operation
}

// ResolvedRelationReference is an Operation that is a resolved relation reference.
// It contains both the name and the actual RelationOrPermission being referenced.
type ResolvedRelationReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

	// relationName is the name of the relation or permission being referenced.
	relationName string

	// resolved is the actual RelationOrPermission being referenced.
	resolved RelationOrPermission
}

// ResolvedArrowReference is an Operation that represents a resolved arrow reference.
// It contains the resolved left side relation and the name of the right side.
type ResolvedArrowReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

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

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (e *ExclusionOperation) Parent() Parented {
	return e.parent
}

func (e *ExclusionOperation) setParent(p Parented) {
	e.parent = p
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

// cloneWithParent creates a deep copy of the ExclusionOperation with the specified parent.
func (e *ExclusionOperation) cloneWithParent(parent Operation) Operation {
	if e == nil {
		return nil
	}
	cloned := &ExclusionOperation{
		parent: parent,
	}
	if leftCloner, ok := e.left.(operationCloner); ok {
		cloned.left = leftCloner.cloneWithParent(cloned)
	} else {
		cloned.left = e.left.clone()
	}
	if rightCloner, ok := e.right.(operationCloner); ok {
		cloned.right = rightCloner.cloneWithParent(cloned)
	} else {
		cloned.right = e.right.clone()
	}
	return cloned
}

// RelationName returns the name of the relation or permission being referenced.
func (r *ResolvedRelationReference) RelationName() string {
	return r.relationName
}

// Resolved returns the actual RelationOrPermission being referenced.
func (r *ResolvedRelationReference) Resolved() RelationOrPermission {
	return r.resolved
}

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (r *ResolvedRelationReference) Parent() Parented {
	return r.parent
}

func (r *ResolvedRelationReference) setParent(p Parented) {
	r.parent = p
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

// cloneWithParent creates a deep copy of the ResolvedRelationReference with the specified parent.
func (r *ResolvedRelationReference) cloneWithParent(parent Operation) Operation {
	if r == nil {
		return nil
	}
	return &ResolvedRelationReference{
		parent:       parent,
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

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (a *ResolvedArrowReference) Parent() Parented {
	return a.parent
}

func (a *ResolvedArrowReference) setParent(p Parented) {
	a.parent = p
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

// cloneWithParent creates a deep copy of the ResolvedArrowReference with the specified parent.
func (a *ResolvedArrowReference) cloneWithParent(parent Operation) Operation {
	if a == nil {
		return nil
	}
	return &ResolvedArrowReference{
		parent:       parent,
		left:         a.left,
		resolvedLeft: a.resolvedLeft,
		right:        a.right,
	}
}

// ResolvedFunctionedArrowReference is an Operation that represents a resolved functioned arrow reference.
// It contains the resolved left side relation, the name of the right side, and the function type.
type ResolvedFunctionedArrowReference struct {
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

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

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (a *ResolvedFunctionedArrowReference) Parent() Parented {
	return a.parent
}

func (a *ResolvedFunctionedArrowReference) setParent(p Parented) {
	a.parent = p
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

// cloneWithParent creates a deep copy of the ResolvedFunctionedArrowReference with the specified parent.
func (a *ResolvedFunctionedArrowReference) cloneWithParent(parent Operation) Operation {
	if a == nil {
		return nil
	}
	return &ResolvedFunctionedArrowReference{
		parent:       parent,
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
	// parent is the parent operation in the operation tree, or the Permission for root operations.
	parent Parented

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

// Parent returns the parent operation in the operation tree, or the Permission for the root operation.
func (f *FunctionedArrowReference) Parent() Parented {
	return f.parent
}

func (f *FunctionedArrowReference) setParent(p Parented) {
	f.parent = p
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

// cloneWithParent creates a deep copy of the FunctionedArrowReference with the specified parent.
func (f *FunctionedArrowReference) cloneWithParent(parent Operation) Operation {
	if f == nil {
		return nil
	}
	return &FunctionedArrowReference{
		parent:   parent,
		left:     f.left,
		right:    f.right,
		function: f.function,
	}
}

// setParent sets the parent pointer for a Parented element.
// The parent parameter can be either an Operation (for child nodes) or a *Permission (for the root node).
// This is a simple setter that delegates to the element's setParent method.
func setParent(elem Parented, parent Parented) {
	if elem == nil {
		return
	}
	elem.setParent(parent)
}

// setChildrenParent sets the parent for all children of a composite operation.
func setChildrenParent(children []Operation, parent Operation) {
	for _, child := range children {
		if child != nil {
			child.setParent(parent)
		}
	}
}

// We close the enum by implementing the private method.
func (r *RelationReference) isOperation()                {}
func (n *NilReference) isOperation()                     {}
func (s *SelfReference) isOperation()                    {}
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
