package query

// LocalExecutor is the simplest executor.
// It simply calls the iterator's implementation directly.
type LocalExecutor struct{}

var _ Executor = LocalExecutor{}

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resourceIDs` are connected to `subjectID`.
// Returns the sequence of matching relations, if they exist, at most `len(resourceIDs)`.
func (l LocalExecutor) Check(ctx *Context, it Iterator, resourceIDs []string, subjectID string) (RelationSeq, error) {
	return it.CheckImpl(ctx, resourceIDs, subjectID)
}

// IterSubjects returns a sequence of all the relations in this set that match the given resourceID.
func (l LocalExecutor) IterSubjects(ctx *Context, it Iterator, resourceID string) (RelationSeq, error) {
	return it.IterSubjectsImpl(ctx, resourceID)
}

// IterResources returns a sequence of all the relations in this set that match the given subjectID.
func (l LocalExecutor) IterResources(ctx *Context, it Iterator, subjectID string) (RelationSeq, error) {
	return it.IterResourcesImpl(ctx, subjectID)
}
