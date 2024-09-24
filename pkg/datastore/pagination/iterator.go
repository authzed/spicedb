package pagination

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewPaginatedIterator creates an implementation of the datastore.Iterator
// interface that internally paginates over datastore results.
func NewPaginatedIterator(
	ctx context.Context,
	reader datastore.Reader,
	filter datastore.RelationshipsFilter,
	pageSize uint64,
	order options.SortOrder,
	startCursor options.Cursor,
) (datastore.RelationshipIterator, error) {
	pi := &paginatedIterator{
		ctx:      ctx,
		reader:   reader,
		filter:   filter,
		pageSize: pageSize,
		order:    order,
		delegate: common.NewSliceRelationshipIterator(nil, options.ByResource),
	}

	pi.startNewBatch(startCursor)

	return pi, pi.err
}

type paginatedIterator struct {
	ctx      context.Context
	reader   datastore.Reader
	filter   datastore.RelationshipsFilter
	pageSize uint64
	order    options.SortOrder

	delegate          datastore.RelationshipIterator
	returnedFromBatch uint64
	err               error
	closed            bool
}

func (pi *paginatedIterator) Next() *core.RelationTuple {
	if pi.Err() != nil {
		return nil
	}

	var next *core.RelationTuple
	for next = pi.delegate.Next(); next == nil; next = pi.delegate.Next() {
		if pi.delegate.Err() != nil {
			pi.err = pi.delegate.Err()
			return nil
		}

		if pi.returnedFromBatch < pi.pageSize {
			// No more tuples to get
			return nil
		}

		cursor, err := pi.delegate.Cursor()
		if err != nil {
			if errors.Is(err, datastore.ErrCursorEmpty) {
				// The last batch had no data
				return nil
			}

			pi.err = err
			return nil
		}

		pi.startNewBatch(cursor)
		if pi.err != nil {
			return nil
		}
	}

	pi.returnedFromBatch++
	return next
}

func (pi *paginatedIterator) startNewBatch(cursor options.Cursor) {
	pi.delegate.Close()
	pi.returnedFromBatch = 0
	pi.delegate, pi.err = pi.reader.QueryRelationships(
		pi.ctx,
		pi.filter,
		options.WithSort(pi.order),
		options.WithLimit(&pi.pageSize),
		options.WithAfter(cursor),
	)
}

func (pi *paginatedIterator) Cursor() (options.Cursor, error) {
	return pi.delegate.Cursor()
}

func (pi *paginatedIterator) Err() error {
	switch {
	case pi.closed:
		return datastore.ErrClosedIterator
	case pi.err != nil:
		return pi.err
	case pi.ctx.Err() != nil:
		return pi.ctx.Err()
	default:
		return nil
	}
}

func (pi *paginatedIterator) Close() {
	pi.closed = true
	if pi.delegate != nil {
		pi.delegate.Close()
	}
}
