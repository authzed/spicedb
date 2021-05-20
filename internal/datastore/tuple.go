package datastore

import (
	"errors"
	"fmt"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

var (
	errClosedIterator = errors.New("unable to iterate: iterator closed")
)

// NewSliceTupleIterator creates a datastore.TupleIterator instance from a materialized slice of tuples.
func NewSliceTupleIterator(tuples []*pb.RelationTuple) TupleIterator {
	return &sliceTupleIterator{tuples: tuples}
}

type sliceTupleIterator struct {
	tuples []*pb.RelationTuple
	closed bool
	err    error
}

// Next implements TupleIterator
func (sti *sliceTupleIterator) Next() *pb.RelationTuple {
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
func BuildFinalizerFunction(sql string, args []interface{}) func(iter *sliceTupleIterator) {
	return func(iter *sliceTupleIterator) {
		if !iter.closed {
			panic(fmt.Sprintf(
				"Tuple iterator garbage collected before Close() was called\n sql: %s\n args: %#v\n",
				sql,
				args,
			))
		}
	}
}
