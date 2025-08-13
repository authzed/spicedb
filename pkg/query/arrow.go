package query


type Arrow struct {
	left  Iterator
	right Iterator
	// TODO(barakmich): strategy field -- this is how the statistics affect the plan
}

var _ Iterator = &Arrow{}

func NewArrow(left, right Iterator) *Arrow {
	return &Arrow{
		left:  left,
		right: right,
	}
}

func (a *Arrow) Check(ctx *Context, resourceIds []string, subjectId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
}

func (a *Arrow) LookupSubjects(ctx *Context, resourceId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
}

func (a *Arrow) LookupResources(ctx *Context, subjectId string) (RelationSeq, error) {
	panic("not implemented") // TODO: Implement
}

func (a *Arrow) Explain() Explain {
	return Explain{
		Info:       "Arrow",
		SubExplain: []Explain{a.left.Explain(), a.right.Explain()},
	}
}
