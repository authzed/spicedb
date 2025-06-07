package cursorediterator

import (
	"context"
	"iter"
	"strconv"

	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	completedHeaderValue              = -1
	parallelIteratorResultsBufferSize = 100
	producerChunksBufferSize          = 10
)

// CursoredWithIntegerHeader is a function that takes a Cursor, a `header` iterator,
// a `next` iterator and executes the `header` iterator with a Cursor of an integer value.
//
// The integer value found at the HEAD of the Cursor represents the starting index for the next
// iteration of the `header` iterator, if any. For example, if the `header` iterator last yields
// a value of 5, then the returned Cursor will have an entry of "6", indicating that the
// `header` iterator should start yielding values *from* index 6.
//
// If the Cursor is empty, the `header` iterator will be called with an index of 0.
//
// If the `header` iterator has completed all its values, then the HEAD of Cursor will be -1,
// and the `header` iterator will not be called again.
//
// Unlike the other blocks, the `header` iterator does not have to return the next Cursor at all,
// as yielded items are automatically decorated with the next item's index.
func CursoredWithIntegerHeader[I any](
	ctx context.Context,
	currentCursor Cursor,
	header func(ctx context.Context, startIndex int) iter.Seq2[I, error],
	next Next[I],
) iter.Seq2[ItemAndCursor[I], error] {
	nextStartingIndex, remainingCursor, err := CursorIntHeadValue(currentCursor)
	if err != nil {
		return yieldsError[ItemAndCursor[I]](err)
	}

	// nextWithAdjustedCursor is a function that returns an iterator sequence
	// that yields the next items from the `next` iterator, adjusting the Cursor
	// by prefixing the Cursor with "-1" to indicate that the header iterator has completed.
	nextWithAdjustedCursor := func(yield func(ItemAndCursor[I], error) bool) {
		for r := range next(ctx, remainingCursor) {
			if !yield(r.withCursorPrefix("-1"), nil) {
				return
			}
		}
	}

	// If nextStartingIndex is -1, it means that the `header` iterator has completed all its values,
	// and we should not call it again.
	if nextStartingIndex == completedHeaderValue {
		return nextWithAdjustedCursor
	}

	// CursorAddingHeader is a function that automatically decorates any
	// values yielded by the `header` iterator with a Cursor that is
	// prefixed with the next item's index.
	cursorAddingHeader := func(yield func(ItemAndCursor[I], error) bool) {
		currentIndex := nextStartingIndex
		for r := range header(ctx, nextStartingIndex) {
			nextIndexStr := strconv.Itoa(currentIndex + 1)
			if !yield(ItemAndCursor[I]{
				Item:   r,
				Cursor: Cursor{nextIndexStr},
			}, nil) {
				return
			}
			currentIndex++
		}
	}

	// Otherwise, we call the `header` iterator with the nextStartingIndex, and then return
	// the next iterator with the remaining Cursor.
	return join(cursorAddingHeader, nextWithAdjustedCursor)
}

// itemOrError is a struct that holds an item or an error.
type itemOrError[I any] struct {
	ItemAndCursor ItemAndCursor[I]
	err           error
}

// CursoredParallelIterators is a function that takes a context, a Cursor, a concurrency level,
// and a list of `next` iterators, which are executed in parallel but whose results
// are yielded in the order matching the order of the iterators.
//
// The integer value found at the HEAD of the Cursor represents the starting iterator to execute
// in the list of `orderedIterators` iterators. If the Cursor is empty, all iterators in the list
// will be executed in parallel.
//
// Yielded items are automatically decorated with the correct prefix for the Cursor, representing
// the index of the iterator in the list of `orderedIterators` iterators.
func CursoredParallelIterators[I any](
	ctx context.Context,
	currentCursor Cursor,
	concurrency uint16,
	orderedIterators ...Next[I],
) iter.Seq2[ItemAndCursor[I], error] {
	if len(orderedIterators) == 0 {
		return Empty[I](ctx, currentCursor)
	}

	ctx, cancel := context.WithCancel(ctx)

	// Find the starting index for the next iterator to execute.
	currentStartingBranchIndex, remainingCursor, err := CursorIntHeadValue(currentCursor)
	if err != nil {
		cancel()
		return yieldsError[ItemAndCursor[I]](err)
	}

	if currentStartingBranchIndex >= len(orderedIterators) {
		cancel()
		return yieldsError[ItemAndCursor[I]](spiceerrors.MustBugf("invalid starting index %d for %d iterators", currentStartingBranchIndex, len(orderedIterators)))
	}

	itersToRun := orderedIterators[currentStartingBranchIndex:]

	// Build a task runner that will execute the iterators in parallel, collecting their results
	// into channels.
	tr := taskrunner.NewPreloadedTaskRunner(ctx, concurrency, len(itersToRun))
	collectors := make([]chan itemOrError[I], len(itersToRun))
	for i, iter := range itersToRun {
		i := i
		iter := iter
		collector := make(chan itemOrError[I], parallelIteratorResultsBufferSize)
		collectors[i] = collector

		// If not the first iterator being executed by this invocation, then we set
		// the remaining Cursor to nil, as it cannot apply to subsequent iterators,
		// as any values in the current Cursor were previously yielded by this
		// iterator only.
		iteratorRemainingCursor := remainingCursor
		if i > 0 {
			iteratorRemainingCursor = nil
		}

		tr.Add(func(ctx context.Context) error {
			for r, err := range iter(ctx, iteratorRemainingCursor) {
				select {
				case <-ctx.Done():
					return nil

				default:
					collector <- itemOrError[I]{r, err}
					if err != nil {
						return err
					}
				}
			}

			close(collector)
			return nil
		})
	}

	return func(yield func(ItemAndCursor[I], error) bool) {
		// Cancel all work once the iterator completes.
		defer cancel()

		// Start the tassk runner.
		tr.Start()

	parent:
		for collectorOffsetIndex, collector := range collectors {
			for {
				select {
				case <-ctx.Done():
					return

				case r, ok := <-collector:
					if !ok {
						continue parent
					}

					if r.err != nil {
						// If the iterator returned an error, cancel all additional work and return the error.
						cancel()
						if !yield(ItemAndCursor[I]{}, r.err) {
							return
						}
						return
					}

					// Otherwise, yield the item and the adjusted Cursor, which is the current overall
					// branch index.
					collectorIndex := currentStartingBranchIndex + collectorOffsetIndex
					if !yield(ItemAndCursor[I]{
						Item:   r.ItemAndCursor.Item,
						Cursor: r.ItemAndCursor.Cursor.withPrefix(strconv.Itoa(collectorIndex)),
					}, nil) {
						return
					}
				}
			}
		}
	}
}

// ChunkAndFollowCursor is a struct that holds a chunk of items and the portion of the Cursor
// following the chunk, indicating where the next chunk should start.
type ChunkAndFollowCursor[P any, C any] struct {
	Chunk  []P
	Follow C
}

// chunkOrError is a struct that holds a chunk of items and an error, or indicates that the chunking has completed.
type chunkOrError[P any, C any] struct {
	chunk     ChunkAndFollowCursor[P, C]
	err       error
	completed bool
}

// CursoredProducerMapperIterator is an iterator that takes a chunk of items produced
// by a `producer` iterator and maps them to items of type `I` using a `mapper` function.
// The `producer` function runs in parallel with the `mapper`.
func CursoredProducerMapperIterator[C any, P any, I any](
	ctx context.Context,
	currentCursor Cursor,
	cursorFromStringConverter CursorFromStringConverter[C],
	cursorToStringConverter func(C) string,
	producer func(ctx context.Context, startIndex C) iter.Seq2[ChunkAndFollowCursor[P, C], error],
	mapper func(ctx context.Context, remainingCursor Cursor, chunk []P) iter.Seq2[ItemAndCursor[I], error],
) iter.Seq2[ItemAndCursor[I], error] {
	ctx, cancel := context.WithCancel(ctx)

	headValue, remainingCursor, err := CursorCustomHeadValue(currentCursor, cursorFromStringConverter)
	if err != nil {
		cancel()
		return yieldsError[ItemAndCursor[I]](err)
	}

	chunks := make(chan chunkOrError[P, C], producerChunksBufferSize)
	producerFunc := func() {
		for p, err := range producer(ctx, headValue) {
			select {
			case <-ctx.Done():
				return

			default:
				chunks <- chunkOrError[P, C]{chunk: p, err: err}
				if err != nil {
					return
				}
			}
		}

		chunks <- chunkOrError[P, C]{completed: true}
	}

	return func(yield func(ItemAndCursor[I], error) bool) {
		defer cancel()

		// Start the producer in a goroutine.
		go producerFunc()

		chunkRemainingCursor := remainingCursor

	outer:
		for {
			select {
			case <-ctx.Done():
				return

			case chunkOrError, ok := <-chunks:
				if !ok {
					return
				}

				if chunkOrError.completed {
					break outer
				}

				if chunkOrError.err != nil {
					cancel()
					if !yield(ItemAndCursor[I]{}, chunkOrError.err) {
						return
					}
					return
				}

				for r := range mapper(ctx, chunkRemainingCursor, chunkOrError.chunk.Chunk) {
					if !yield(ItemAndCursor[I]{
						Item:   r.Item,
						Cursor: r.Cursor.withPrefix(cursorToStringConverter(chunkOrError.chunk.Follow)),
					}, nil) {
						return
					}
				}

				// Once the first chunk is processed, we set the remaining Cursor to nil, as it was
				// indexing into the first chunk.
				chunkRemainingCursor = nil
			}
		}
	}
}
