package common

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewSliceRelationshipIterator creates a datastore.TupleIterator instance from a materialized slice of tuples.
func NewSliceRelationshipIterator(rels []tuple.Relationship, order options.SortOrder) datastore.RelationshipIterator {
	iter := &sliceRelationshipIterator{rels: rels, order: order}
	spiceerrors.SetFinalizerForDebugging(iter, MustIteratorBeClosed)
	return iter
}

type sliceRelationshipIterator struct {
	rels   []tuple.Relationship
	order  options.SortOrder
	last   tuple.Relationship
	closed bool
	err    error
}

// Next implements TupleIterator
func (sti *sliceRelationshipIterator) Next() (tuple.Relationship, bool) {
	if sti.closed {
		sti.err = datastore.ErrClosedIterator
		return tuple.Relationship{}, false
	}

	if len(sti.rels) > 0 {
		first := sti.rels[0]
		sti.rels = sti.rels[1:]
		sti.last = first
		return first, true
	}

	return tuple.Relationship{}, false
}

func (sti *sliceRelationshipIterator) Cursor() (options.Cursor, error) {
	switch {
	case sti.closed:
		return nil, datastore.ErrClosedIterator
	case sti.order == options.Unsorted:
		return nil, datastore.ErrCursorsWithoutSorting
	case sti.last.Resource.ObjectType == "":
		return nil, datastore.ErrCursorEmpty
	default:
		return options.ToCursor(sti.last), nil
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

	sti.rels = nil
	sti.closed = true
}

// MustIteratorBeClosed is a function which can be used as a finalizer to make sure that
// tuples are getting closed before they are garbage collected.
func MustIteratorBeClosed(iter *sliceRelationshipIterator) {
	if !iter.closed {
		panic("Relationship iterator garbage collected before Close() was called")
	}
}
