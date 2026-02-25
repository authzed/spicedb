package query

import "time"

type ObserverOperation int

const (
	CheckOperation ObserverOperation = iota
	IterSubjectsOperation
	IterResourcesOperation
)

type Observer interface {
	ObserveTiming(op ObserverOperation, key CanonicalKey, dur time.Duration)
	ObservePath(op ObserverOperation, key CanonicalKey, path Path)
	ObserveEnterIterator(op ObserverOperation, key CanonicalKey)
	ObserveReturnIterator(op ObserverOperation, key CanonicalKey)
}
