package query

// Operation identifies which query operation is being executed.
// The zero value (OperationUnset) means no operation has been set yet.
type Operation int

const (
	// OperationUnset is the zero value; no operation has started yet.
	OperationUnset Operation = iota
	// Check means the operation is a point-lookup check.
	Check
	// IterSubjects means the operation enumerates subjects for a given resource.
	IterSubjects
	// IterResources means the operation enumerates resources for a given subject.
	IterResources
)

type Observer interface {
	ObserveEnterIterator(op Operation, key CanonicalKey)
	ObservePath(op Operation, key CanonicalKey, path *Path)
	ObserveReturnIterator(op Operation, key CanonicalKey)
}
