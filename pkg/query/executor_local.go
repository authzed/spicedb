package query

// LocalExecutor is the simplest executor.
// It simply calls the iterator's implementation directly.
type LocalExecutor struct{}

var _ Executor = LocalExecutor{}

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resources` are connected to `subject`.
// Returns the sequence of matching relations, if they exist, at most `len(resources)`.
func (l LocalExecutor) Check(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	return it.CheckImpl(ctx, resources, subject)
}

// IterSubjects returns a sequence of all the relations in this set that match the given resource.
func (l LocalExecutor) IterSubjects(ctx *Context, it Iterator, resource Object) (RelationSeq, error) {
	return it.IterSubjectsImpl(ctx, resource)
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
func (l LocalExecutor) IterResources(ctx *Context, it Iterator, subject ObjectAndRelation) (RelationSeq, error) {
	return it.IterResourcesImpl(ctx, subject)
}
