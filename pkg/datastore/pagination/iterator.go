package pagination

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
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
	includeObjectData bool,
) (datastore.RelationshipIterator, error) {
	iter, err := reader.QueryRelationships(
		ctx,
		filter,
		options.WithSort(order),
		options.WithLimit(&pageSize),
		options.WithAfter(startCursor),
		options.WithIncludeObjectData(includeObjectData),
	)
	if err != nil {
		return nil, err
	}

	return func(yield func(tuple.Relationship, error) bool) {
		cursor := startCursor
		for {
			var counter uint64
			for rel, err := range iter {
				if !yield(rel, err) {
					return
				}

				cursor = options.ToCursor(rel)
				counter++

				if counter >= pageSize {
					break
				}
			}

			if counter < pageSize {
				return
			}

			iter, err = reader.QueryRelationships(
				ctx,
				filter,
				options.WithSort(order),
				options.WithLimit(&pageSize),
				options.WithAfter(cursor),
				options.WithIncludeObjectData(includeObjectData),
			)
			if err != nil {
				yield(tuple.Relationship{}, err)
				return
			}
		}
	}, nil
}
