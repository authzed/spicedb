package datastore

import (
	"errors"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var errClosedIterator = errors.New("unable to iterate: iterator closed")

// NewSliceTupleIterator creates a datastore.TupleIterator instance from a materialized slice of tuples.
func NewSliceTupleIterator(tuples []*core.RelationTuple) TupleIterator {
	return &sliceTupleIterator{tuples: tuples}
}

type sliceTupleIterator struct {
	tuples []*core.RelationTuple
	closed bool
	err    error
}

// Next implements TupleIterator
func (sti *sliceTupleIterator) Next() *core.RelationTuple {
	if sti.closed {
		sti.err = errClosedIterator
		return nil
	}

	if len(sti.tuples) > 0 {
		first := sti.tuples[0]
		sti.tuples = sti.tuples[1:]
		return first
	}

	return nil
}

// Err implements TupleIterator
func (sti *sliceTupleIterator) Err() error {
	return sti.err
}

// Close implements TupleIterator
func (sti *sliceTupleIterator) Close() {
	if sti.closed {
		panic("tuple iterator double closed")
	}

	sti.tuples = nil
	sti.closed = true
}

// BuildFinalizerFunction creates a function which can be used as a finalizer to make sure that
// tuples are getting closed before they are garbage collected.
func BuildFinalizerFunction() func(iter *sliceTupleIterator) {
	return func(iter *sliceTupleIterator) {
		if !iter.closed {
			panic("Tuple iterator garbage collected before Close() was called")
		}
	}
}
