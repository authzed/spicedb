package schema

type CaveatParameter struct {
	TypeName   string
	ChildTypes []*CaveatParameter
}

type UnionOperation struct {
	Children []Operation
}

type Operation interface {
	isOperation()
}

type RelationReference struct {
	RelationName string
}

type ArrowReference struct {
	Left  string
	Right string
}

type IntersectionOperation struct {
	Children []Operation
}

type ExclusionOperation struct {
	Left  Operation
	Right Operation
}

func (r *RelationReference) isOperation()     {}
func (r *ArrowReference) isOperation()        {}
func (u *UnionOperation) isOperation()        {}
func (i *IntersectionOperation) isOperation() {}
func (e *ExclusionOperation) isOperation()    {}

