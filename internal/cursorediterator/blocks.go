package cursorediterator

import (
	"context"
	"iter"
	"strconv"

	"github.com/authzed/ctxkey"

	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	completedHeaderValue              = -1
	parallelIteratorResultsBufferSize = 1000
	producerChunksBufferSize          = 1000
	mapperItemsSize                   = 1000
)

var cursorsDisabledKey = ctxkey.NewBoxedWithDefault(false)

// DisableCursorsInContext returns a new context with cursors disabled. When cursors are disabled,
// all cursored iterator functions will return nil cursors instead of constructing them.
func DisableCursorsInContext(ctx context.Context) context.Context {
	ctx = cursorsDisabledKey.SetBox(ctx)
	cursorsDisabledKey.Set(ctx, true)
	return ctx
}

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
	// Check for context cancellation before any processing
	if ctx.Err() != nil {
		return YieldsError[ItemAndCursor[I]](ctx.Err())
	}

	// Check if cursors are disabled
	cursorsDisabled := cursorsDisabledKey.Value(ctx)

	nextStartingIndex, remainingCursor, err := CursorIntHeadValue(currentCursor)
	if err != nil {
		return YieldsError[ItemAndCursor[I]](err)
	}

	// nextWithAdjustedCursor is a function that returns an iterator sequence
	// that yields the next items from the `next` iterator, adjusting the Cursor
	// by adding the Cursor with "-1" to indicate that the header iterator has completed.
	nextWithAdjustedCursor := func(yield func(ItemAndCursor[I], error) bool) {
		// Check for context cancellation before executing next
		if ctx.Err() != nil {
			_ = yield(ItemAndCursor[I]{}, ctx.Err())
			return
		}

		for r, err := range next(ctx, remainingCursor) {
			if err != nil {
				_ = yield(ItemAndCursor[I]{}, err)
				return
			}

			// Check for context cancellation during next execution
			if ctx.Err() != nil {
				_ = yield(ItemAndCursor[I]{}, ctx.Err())
				return
			}

			var cursor Cursor
			if !cursorsDisabled {
				cursor = r.Cursor.withHead("-1")
			}
			if !yield(ItemAndCursor[I]{Item: r.Item, Cursor: cursor}, nil) {
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
		// Check for context cancellation before executing header
		if ctx.Err() != nil {
			_ = yield(ItemAndCursor[I]{}, ctx.Err())
			return
		}

		currentIndex := nextStartingIndex
		for r, err := range header(ctx, nextStartingIndex) {
			if err != nil {
				_ = yield(ItemAndCursor[I]{}, err)
				return
			}

			// Check for context cancellation during header execution
			if ctx.Err() != nil {
				_ = yield(ItemAndCursor[I]{}, ctx.Err())
				return
			}

			var cursor Cursor
			if !cursorsDisabled {
				nextIndexStr := strconv.Itoa(currentIndex + 1)
				cursor = Cursor{nextIndexStr}
			}
			if !yield(ItemAndCursor[I]{
				Item:   r,
				Cursor: cursor,
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

// item is a struct that holds an item or an error.
type item[I any] struct {
	itemAndCursor ItemAndCursor[I]
	err           error
	completed     bool
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

	// Check if cursors are disabled
	cursorsDisabled := cursorsDisabledKey.Value(ctx)

	// Find the starting index for the next iterator to execute.
	currentStartingBranchIndex, remainingCursor, err := CursorIntHeadValue(currentCursor)
	if err != nil {
		return YieldsError[ItemAndCursor[I]](err)
	}

	if currentStartingBranchIndex >= len(orderedIterators) {
		return YieldsError[ItemAndCursor[I]](spiceerrors.MustBugf("invalid starting index %d for %d iterators", currentStartingBranchIndex, len(orderedIterators)))
	}

	itersToRun := orderedIterators[currentStartingBranchIndex:]

	// Special case for concurrency=1: execute sequentially without goroutines
	if concurrency == 1 {
		return func(yield func(ItemAndCursor[I], error) bool) {
			// Check for context cancellation before starting execution
			if ctx.Err() != nil {
				_ = yield(ItemAndCursor[I]{}, ctx.Err())
				return
			}

			for collectorOffsetIndex, iter := range itersToRun {
				// Check for context cancellation before each iterator
				if ctx.Err() != nil {
					_ = yield(ItemAndCursor[I]{}, ctx.Err())
					return
				}

				iteratorRemainingCursor := remainingCursor
				if collectorOffsetIndex > 0 {
					iteratorRemainingCursor = nil
				}

				collectorIndex := currentStartingBranchIndex + collectorOffsetIndex
				collectorIndexStr := strconv.Itoa(collectorIndex)
				for r, err := range iter(ctx, iteratorRemainingCursor) {
					if err != nil {
						_ = yield(ItemAndCursor[I]{}, err)
						return
					}

					if ctx.Err() != nil {
						_ = yield(ItemAndCursor[I]{}, ctx.Err())
						return
					}

					var cursor Cursor
					if !cursorsDisabled {
						cursor = r.Cursor.withHead(collectorIndexStr)
					}
					if !yield(ItemAndCursor[I]{
						Item:   r.Item,
						Cursor: cursor,
					}, nil) {
						return
					}
				}
			}
		}
	}

	// Build a task runner that will execute the iterators in parallel, collecting their results
	// into channels.
	ctx, cancel := context.WithCancel(ctx)

	tr := taskrunner.NewPreloadedTaskRunner(ctx, concurrency, len(itersToRun))
	collectors := make([]chan item[I], len(itersToRun))
	for i, iter := range itersToRun {
		i := i
		iter := iter
		collector := make(chan item[I], parallelIteratorResultsBufferSize)
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
					return ctx.Err()

				// Propagate the result and the error up to the collector
				case collector <- item[I]{r, err, false}:
					if err != nil {
						// Stop processing if an error was encountered
						return nil
					}
					// Otherwise continue
					continue
				}
			}

			collector <- item[I]{completed: true}
			return nil
		})
	}

	return func(yield func(ItemAndCursor[I], error) bool) {
		// Cancel all work once the iterator completes.
		defer cancel()

		// Start the task runner.
		tr.Start()

	parent:
		for collectorOffsetIndex := range collectors {
			collector := collectors[collectorOffsetIndex]

			for {
				select {
				case <-ctx.Done():
					_ = yield(ItemAndCursor[I]{}, ctx.Err())
					return

				case r := <-collector:
					if r.err != nil {
						_ = yield(ItemAndCursor[I]{}, r.err)
						return
					}

					if r.completed {
						continue parent
					}

					// Otherwise, yield the item and the adjusted Cursor, which is the current overall
					// branch index.
					collectorIndex := currentStartingBranchIndex + collectorOffsetIndex
					var cursor Cursor
					if !cursorsDisabled {
						cursor = r.itemAndCursor.Cursor.withHead(strconv.Itoa(collectorIndex))
					}
					if !yield(ItemAndCursor[I]{
						Item:   r.itemAndCursor.Item,
						Cursor: cursor,
					}, nil) {
						return
					}
				}
			}
		}
	}
}

// ChunkOrHold is an enum-like type that can hold one of three values:
// - Chunk: contains a chunk and the cursor position for the current chunk
// - HoldForMappingComplete: signals that the producer should wait for mapper completion
type ChunkOrHold[K any, C any] interface {
	chunkOrHold()
}

// Chunk holds a chunk of items and the portion of the Cursor
// for the chunk, indicating where the current chunk should resume.
type Chunk[K any, C any] struct {
	CurrentChunk       K
	CurrentChunkCursor C
}

func (Chunk[K, C]) chunkOrHold() {}

// HoldForMappingComplete signals that the producer should wait until
// the mapper has completed processing its current chunk before producing more.
type HoldForMappingComplete[K any, C any] struct{}

func (HoldForMappingComplete[K, C]) chunkOrHold() {}

// producerChunkOrError is a struct that holds a chunk of items or an error or indicates that the chunking has completed.
type producerChunkOrError[P any, C any, I any] struct {
	producerCompleted bool

	err                     error
	chunkAndCursor          Chunk[P, C]
	mappedResultChan        chan mapperResultOrError[I, C]
	wereMappingsYieldedChan chan bool
}

// mapperResultOrError represents the result of mapper processing
type mapperResultOrError[I any, C any] struct {
	items []ItemAndCursor[I]
	err   error
}

// CursoredProducerMapperIterator is an iterator that takes a chunk of items produced
// by a `producer` iterator and maps them to items of type `I` using a `mapper` function.
// The `producer` function runs in parallel with the `mapper`.
// The `mapper` function runs with the specified concurrency level for parallel processing.
// If the producer yields HoldForMappingComplete, further calls to the producer will wait
// until the mapper has completed its current chunk.
func CursoredProducerMapperIterator[C any, P any, I any](
	ctx context.Context,
	currentCursor Cursor,
	concurrency uint16,
	cursorFromStringConverter CursorFromStringConverter[C],
	cursorToStringConverter func(C) (string, error),
	producer func(ctx context.Context, currentIndex C, remainingCursor Cursor) iter.Seq2[ChunkOrHold[P, C], error],
	mapper func(ctx context.Context, remainingCursor Cursor, chunk P) iter.Seq2[ItemAndCursor[I], error],
) iter.Seq2[ItemAndCursor[I], error] {
	// Check for context cancellation before any processing
	if ctx.Err() != nil {
		return YieldsError[ItemAndCursor[I]](ctx.Err())
	}

	ctx, cancel := context.WithCancel(ctx)

	// Check if cursors are disabled
	cursorsDisabled := cursorsDisabledKey.Value(ctx)

	headValue, remainingCursor, err := CursorCustomHeadValue(currentCursor, cursorFromStringConverter)
	if err != nil {
		cancel()
		return YieldsError[ItemAndCursor[I]](err)
	}

	// Special case for concurrency=1: execute sequentially without goroutines
	if concurrency == 1 {
		return func(yield func(ItemAndCursor[I], error) bool) {
			defer cancel()

			for p, err := range producer(ctx, headValue, remainingCursor) {
				if err != nil {
					_ = yield(ItemAndCursor[I]{}, err)
					return
				}

				if ctx.Err() != nil {
					_ = yield(ItemAndCursor[I]{}, ctx.Err())
					return
				}

				// Skip HoldForMappingComplete in sequential mode since there's no parallelism to synchronize
				switch t := p.(type) {
				case HoldForMappingComplete[P, C]:
					continue

				case Chunk[P, C]:
					cursorStr, err := cursorToStringConverter(t.CurrentChunkCursor)
					if err != nil {
						_ = yield(ItemAndCursor[I]{}, err)
						return
					}

					// Process the chunk directly with the mapper
					for r, err := range mapper(ctx, remainingCursor, t.CurrentChunk) {
						if err != nil {
							_ = yield(ItemAndCursor[I]{}, err)
							return
						}

						if ctx.Err() != nil {
							_ = yield(ItemAndCursor[I]{}, ctx.Err())
							return
						}

						var cursor Cursor
						if !cursorsDisabled {
							cursor = r.Cursor.withHead(cursorStr)
						}
						if !yield(ItemAndCursor[I]{
							Item:   r.Item,
							Cursor: cursor,
						}, nil) {
							return
						}
					}

					// Only use the remaining cursor for the first chunk
					remainingCursor = nil

				default:
					if !yield(ItemAndCursor[I]{}, spiceerrors.MustBugf("unknown ChunkFollowOrHold type: %T", t)) {
						return
					}
				}
			}
		}
	}

	orderedProducerChunks := make(chan *producerChunkOrError[P, C, I], producerChunksBufferSize)

	// Helper function to check if a chunk is HoldForMappingComplete
	isHoldForMappingComplete := func(chunk ChunkOrHold[P, C]) bool {
		_, ok := chunk.(HoldForMappingComplete[P, C])
		return ok
	}

	// Create a TaskRunner for managing mapper tasks
	tr := taskrunner.NewTaskRunner(ctx, concurrency)

	mapperTaskFunc := func(coe *producerChunkOrError[P, C, I], chunkRemainingCursor Cursor) taskrunner.TaskFunc {
		return func(ctx context.Context) error {
			items := make([]ItemAndCursor[I], 0, mapperItemsSize)
			for r, err := range mapper(ctx, chunkRemainingCursor, coe.chunkAndCursor.CurrentChunk) {
				if err != nil {
					coe.mappedResultChan <- mapperResultOrError[I, C]{err: err}
					return nil
				}
				items = append(items, r)
			}
			coe.mappedResultChan <- mapperResultOrError[I, C]{items: items}
			return nil
		}
	}

	producerFunc := func(ctx context.Context) error {
		var precedingConcreteChunk *producerChunkOrError[P, C, I]
		chunkRemainingCursor := remainingCursor

		for p, err := range producer(ctx, headValue, remainingCursor) {
			if err != nil {
				orderedProducerChunks <- &producerChunkOrError[P, C, I]{err: err}
				return nil
			}

			// If a HoldForMappingComplete is yielded, we need to wait for the *mapper* to complete
			// *yielding* the previous chunk before yielding more chunks.
			if isHoldForMappingComplete(p) {
				if precedingConcreteChunk == nil {
					// This is an error state: we cannot yield a HoldForMappingComplete
					// before yielding any chunks. We should return an error to the caller.
					orderedProducerChunks <- &producerChunkOrError[P, C, I]{err: spiceerrors.MustBugf("HoldForMappingComplete cannot be yielded before any chunks")}
					return nil
				}

				select {
				case <-ctx.Done():
					return ctx.Err()

				case <-precedingConcreteChunk.wereMappingsYieldedChan:
					precedingConcreteChunk = nil
					continue
				}
			}

			chunk := &producerChunkOrError[P, C, I]{
				chunkAndCursor: p.(Chunk[P, C]),

				mappedResultChan:        make(chan mapperResultOrError[I, C], 1),
				wereMappingsYieldedChan: make(chan bool, 1),
			}
			precedingConcreteChunk = chunk

			select {
			case <-ctx.Done():
				return ctx.Err()

			case orderedProducerChunks <- chunk:
				// Continue processing chunks.
			}

			// Schedule the mapper task for this chunk
			tr.Schedule(mapperTaskFunc(chunk, chunkRemainingCursor))

			// Remaining cursor is only used for the first chunk
			chunkRemainingCursor = nil
		}

		// If we reach here, the producer has completed yielding chunks.
		// We need to signal that the last chunk has been completed.
		chunk := &producerChunkOrError[P, C, I]{
			producerCompleted: true,
		}
		precedingConcreteChunk = nil

		select {
		case <-ctx.Done():
			return ctx.Err()

		case orderedProducerChunks <- chunk:
			// Send the completion signal.
			return nil
		}
	}

	return func(yield func(ItemAndCursor[I], error) bool) {
		defer cancel()

		// Start the producer.
		tr.Schedule(func(ctx context.Context) error {
			return producerFunc(ctx)
		})

		for {
			select {
			case <-ctx.Done():
				_ = yield(ItemAndCursor[I]{}, ctx.Err())
				return

			case chunk := <-orderedProducerChunks:
				if chunk.err != nil {
					_ = yield(ItemAndCursor[I]{}, chunk.err)
					return
				}
				if chunk.producerCompleted {
					return
				}

				cursorStr, err := cursorToStringConverter(chunk.chunkAndCursor.CurrentChunkCursor)
				if err != nil {
					_ = yield(ItemAndCursor[I]{}, err)
					return
				}

				select {
				case <-ctx.Done():
					_ = yield(ItemAndCursor[I]{}, ctx.Err())
					return

				case mappedResult := <-chunk.mappedResultChan:
					if mappedResult.err != nil {
						_ = yield(ItemAndCursor[I]{}, mappedResult.err)
						return
					}

					for _, r := range mappedResult.items {
						var cursor Cursor
						if !cursorsDisabled {
							cursor = r.Cursor.withHead(cursorStr)
						}
						if !yield(ItemAndCursor[I]{
							Item:   r.Item,
							Cursor: cursor,
						}, nil) {
							return
						}
					}

					// We have yielded all mappings for this chunk, we can continue processing
					// the next chunk.
					chunk.wereMappingsYieldedChan <- true
				}
			}
		}
	}
}
