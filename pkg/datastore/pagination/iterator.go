package pagination

import (
	"context"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewPaginatedIterator creates an implementation of the datastore.Iterator
// interface that internally paginates over datastore results.
func NewPaginatedIterator(
	ctx context.Context,
	reader datalayer.RevisionedReader,
	filter datastore.RelationshipsFilter,
	limit uint64,
	order options.SortOrder,
	startCursor options.Cursor,
	queryShape queryshape.Shape,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	baseOptions := append([]options.QueryOptionsOption{
		options.WithSort(order),
		options.WithLimit(&limit),
		options.WithQueryShape(queryShape),
	}, opts...)

	return func(yield func(tuple.Relationship, error) bool) {
		cursor := startCursor
		for {
			iter, err := reader.QueryRelationships(
				ctx,
				filter,
				append(baseOptions, options.WithAfter(cursor))...,
			)
			if err != nil {
				yield(tuple.Relationship{}, err)
				return
			}

			var counter uint64
			for rel, err := range iter {
				if !yield(rel, err) {
					return
				}

				cursor = options.ToCursor(rel)
				counter++

				if counter >= limit {
					break
				}
			}

			if counter < limit {
				return
			}
		}
	}, nil
}
