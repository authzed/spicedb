package common

import (
	"runtime"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewSliceRelationshipIterator creates a datastore.TupleIterator instance from a materialized slice of tuples.
func NewSliceRelationshipIterator(tuples []*core.RelationTuple, order options.SortOrder) datastore.RelationshipIterator {
	iter := &sliceRelationshipIterator{tuples: tuples, order: order}
	runtime.SetFinalizer(iter, MustIteratorBeClosed)
	return iter
}

type sliceRelationshipIterator struct {
	tuples []*core.RelationTuple
	order  options.SortOrder
	last   *core.RelationTuple
	closed bool
	err    error
}

// Next implements TupleIterator
func (sti *sliceRelationshipIterator) Next() *core.RelationTuple {
	if sti.closed {
		sti.err = datastore.ErrClosedIterator
		return nil
	}

	if len(sti.tuples) > 0 {
		first := sti.tuples[0]
		sti.tuples = sti.tuples[1:]
		sti.last = first
		return first
	}

	return nil
}

func (sti *sliceRelationshipIterator) Cursor() (options.Cursor, error) {
	switch {
	case sti.closed:
		return nil, datastore.ErrClosedIterator
	case sti.order == options.Unsorted:
		return nil, datastore.ErrCursorsWithoutSorting
	case sti.last == nil:
		return nil, datastore.ErrCursorEmpty
	default:
		return sti.last, nil
	}
}

// Err implements TupleIterator
func (sti *sliceRelationshipIterator) Err() error {
	return sti.err
}

// Close implements TupleIterator
func (sti *sliceRelationshipIterator) Close() {
	if sti.closed {
		return
	}

	sti.tuples = nil
	sti.closed = true
}

// MustIteratorBeClosed is a function which can be used as a finalizer to make sure that
// tuples are getting closed before they are garbage collected.
func MustIteratorBeClosed(iter *sliceRelationshipIterator) {
	if !iter.closed {
		panic("Tuple iterator garbage collected before Close() was called")
	}
}
