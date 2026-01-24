package query

// LocalExecutor is the simplest executor.
// It simply calls the iterator's implementation directly.
type LocalExecutor struct{}

var _ Executor = LocalExecutor{}

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resources` are connected to `subject`.
// Returns the sequence of matching paths, if they exist, at most `len(resources)`.
func (l LocalExecutor) Check(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return it.CheckImpl(ctx, resources, subject)
}

// IterSubjects returns a sequence of all the paths in this set that match the given resource.
func (l LocalExecutor) IterSubjects(ctx *Context, it Iterator, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	pathSeq, err := it.IterSubjectsImpl(ctx, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}
	// Apply filtering wrapper - this is where the actual filtering happens
	return FilterSubjectsByType(pathSeq, filterSubjectType), nil
}

// IterResources returns a sequence of all the paths in this set that match the given subject.
func (l LocalExecutor) IterResources(ctx *Context, it Iterator, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	pathSeq, err := it.IterResourcesImpl(ctx, subject, filterResourceType)
	if err != nil {
		return nil, err
	}
	// Apply filtering wrapper - this is where the actual filtering happens
	return FilterResourcesByType(pathSeq, filterResourceType), nil
}
