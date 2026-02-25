package query

type ObserverOperation int

const (
	CheckOperation ObserverOperation = iota
	IterSubjectsOperation
	IterResourcesOperation
)

type Observer interface {
	ObserveEnterIterator(op ObserverOperation, key CanonicalKey)
	ObservePath(op ObserverOperation, key CanonicalKey, path Path)
	ObserveReturnIterator(op ObserverOperation, key CanonicalKey)
}
