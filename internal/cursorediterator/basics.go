package cursorediterator

import (
	"context"
	"iter"
)

// ItemAndCursor represents an item along with its cursor.
type ItemAndCursor[I any] struct {
	// Item is the item of type I.
	Item I

	// Cursor is the cursor for the item, to resume execution within the iterator.
	// For flat iterators, the HEAD value is typically the integer of the index of the
	// *next* item to yield. For nested iterators, the HEAD value is typically an index
	// into the current branch of the iterator tree.
	Cursor Cursor
}

// Next is a function type that takes a remaining cursor and returns an iterator sequence.
type Next[I any] func(ctx context.Context, cursor Cursor) iter.Seq2[ItemAndCursor[I], error]

// Empty is a function that returns an empty iterator sequence.
func Empty[I any](ctx context.Context, cursor Cursor) iter.Seq2[ItemAndCursor[I], error] {
	return func(yield func(ItemAndCursor[I], error) bool) {
		// Check for context cancellation even in empty iterator
		if ctx.Err() != nil {
			_ = yield(ItemAndCursor[I]{}, ctx.Err())
			return
		}
	}
}
