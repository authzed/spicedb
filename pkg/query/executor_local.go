package query

// LocalExecutor is the simplest executor.
// It simply calls the iterator's implementation directly.
type LocalExecutor struct{}

var _ Executor = LocalExecutor{}

// Check tests if the given resource is connected to subject.
// Returns the matching Path if found, or nil if not found.
func (l LocalExecutor) Check(ctx *Context, it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return it.CheckImpl(ctx, resource, subject)
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
