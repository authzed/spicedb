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
	parallelIteratorResultsBufferSize = 1000
	producerChunksBufferSize          = 1000
	concurrentMapperResultsBufferSize = 1000
	mapperItemsSize                   = 1000
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
	select {
	case <-ctx.Done():
		return YieldsError[ItemAndCursor[I]](ctx.Err())

	default:
		// Start the iterator.
	}

	nextStartingIndex, remainingCursor, err := CursorIntHeadValue(currentCursor)
	if err != nil {
		return YieldsError[ItemAndCursor[I]](err)
	}

	// nextWithAdjustedCursor is a function that returns an iterator sequence
	// that yields the next items from the `next` iterator, adjusting the Cursor
	// by prefixing the Cursor with "-1" to indicate that the header iterator has completed.
	nextWithAdjustedCursor := func(yield func(ItemAndCursor[I], error) bool) {
		for r, err := range next(ctx, remainingCursor) {
			if !yield(r.withCursorPrefix("-1"), err) {
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
		for r, err := range header(ctx, nextStartingIndex) {
			if err != nil {
				if !yield(ItemAndCursor[I]{}, err) {
					return
				}
				return
			}

			select {
			case <-ctx.Done():
				if !yield(ItemAndCursor[I]{}, ctx.Err()) {
					return
				}

			default:
				// Continue the iteration.
			}

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
	select {
	case <-ctx.Done():
		return YieldsError[ItemAndCursor[I]](ctx.Err())

	default:
		//  Start the iterator.
	}

	if len(orderedIterators) == 0 {
		return Empty[I](ctx, currentCursor)
	}

	ctx, cancel := context.WithCancel(ctx)

	// Find the starting index for the next iterator to execute.
	currentStartingBranchIndex, remainingCursor, err := CursorIntHeadValue(currentCursor)
	if err != nil {
		cancel()
		return YieldsError[ItemAndCursor[I]](err)
	}

	if currentStartingBranchIndex >= len(orderedIterators) {
		cancel()
		return YieldsError[ItemAndCursor[I]](spiceerrors.MustBugf("invalid starting index %d for %d iterators", currentStartingBranchIndex, len(orderedIterators)))
	}

	itersToRun := orderedIterators[currentStartingBranchIndex:]

	// Special case for concurrency=1: execute sequentially without goroutines
	if concurrency == 1 {
		return func(yield func(ItemAndCursor[I], error) bool) {
			defer cancel()

			for collectorOffsetIndex, iter := range itersToRun {
				iteratorRemainingCursor := remainingCursor
				if collectorOffsetIndex > 0 {
					iteratorRemainingCursor = nil
				}

				collectorIndex := currentStartingBranchIndex + collectorOffsetIndex
				for r, err := range iter(ctx, iteratorRemainingCursor) {
					select {
					case <-ctx.Done():
						return
					default:
						if err != nil {
							if !yield(ItemAndCursor[I]{}, err) {
								return
							}
							return
						}

						if !yield(ItemAndCursor[I]{
							Item:   r.Item,
							Cursor: r.Cursor.withPrefix(strconv.Itoa(collectorIndex)),
						}, nil) {
							return
						}
					}
				}
			}
		}
	}

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

				case collector <- itemOrError[I]{r, err}:
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

// ChunkFollowOrHold is an enum-like type that can hold one of three values:
// - ChunkAndFollow: contains a chunk and the cursor position for the next chunk
// - HoldForMappingComplete: signals that the producer should wait for mapper completion
type ChunkFollowOrHold[K any, C any] interface {
	chunkFollowOrHold()
}

// ChunkAndFollow holds a chunk of items and the portion of the Cursor
// following the chunk, indicating where the next chunk should start.
type ChunkAndFollow[K any, C any] struct {
	Chunk  K
	Follow C
}

func (ChunkAndFollow[K, C]) chunkFollowOrHold() {}

// HoldForMappingComplete signals that the producer should wait until
// the mapper has completed processing its current chunk before producing more.
type HoldForMappingComplete[K any, C any] struct{}

func (HoldForMappingComplete[K, C]) chunkFollowOrHold() {}

// producerChunkOrError is a struct that holds a chunk of items and an error, or indicates that the chunking has completed.
type producerChunkOrError[P any, C any, I any] struct {
	chunkIndex int

	chunkAndFollow ChunkAndFollow[P, C]
	err            error
	completed      bool

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
	producer func(ctx context.Context, currentIndex C, remainingCursor Cursor) iter.Seq2[ChunkFollowOrHold[P, C], error],
	mapper func(ctx context.Context, remainingCursor Cursor, chunk P) iter.Seq2[ItemAndCursor[I], error],
) iter.Seq2[ItemAndCursor[I], error] {
	ctx, cancel := context.WithCancel(ctx)

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
					if !yield(ItemAndCursor[I]{}, err) {
						return
					}
					return
				}

				// Skip HoldForMappingComplete in sequential mode since there's no parallelism to synchronize
				if _, ok := p.(HoldForMappingComplete[P, C]); ok {
					continue
				}

				chunk := p.(ChunkAndFollow[P, C])

				// Process the chunk directly with the mapper
				for r, err := range mapper(ctx, remainingCursor, chunk.Chunk) {
					select {
					case <-ctx.Done():
						return
					default:
						if err != nil {
							if !yield(ItemAndCursor[I]{}, err) {
								return
							}
							return
						}

						cursorStr, err := cursorToStringConverter(chunk.Follow)
						if err != nil {
							if !yield(ItemAndCursor[I]{}, err) {
								return
							}
							return
						}

						if !yield(ItemAndCursor[I]{
							Item:   r.Item,
							Cursor: r.Cursor.withPrefix(cursorStr),
						}, nil) {
							return
						}
					}
				}

				// Only use the remaining cursor for the first chunk
				remainingCursor = nil
			}
		}
	}

	orderedProducerChunks := make(chan *producerChunkOrError[P, C, I], producerChunksBufferSize)

	// Helper function to check if a chunk is HoldForMappingComplete
	isHoldForMappingComplete := func(chunk ChunkFollowOrHold[P, C]) bool {
		_, ok := chunk.(HoldForMappingComplete[P, C])
		return ok
	}

	// Create a TaskRunner for managing mapper tasks
	tr := taskrunner.NewTaskRunner(ctx, concurrency)

	mapperTaskFunc := func(coe *producerChunkOrError[P, C, I], chunkRemainingCursor Cursor) taskrunner.TaskFunc {
		return func(ctx context.Context) error {
			items := make([]ItemAndCursor[I], 0, mapperItemsSize)
			for r, err := range mapper(ctx, chunkRemainingCursor, coe.chunkAndFollow.Chunk) {
				if err != nil {
					coe.mappedResultChan <- mapperResultOrError[I, C]{err: err}
					return err
				}
				items = append(items, r)
			}
			coe.mappedResultChan <- mapperResultOrError[I, C]{items: items}
			return nil
		}
	}

	producerChunksByID := make(map[int]*producerChunkOrError[P, C, I], producerChunksBufferSize)
	producerFunc := func() {
		chunkIndex := 0

		for p, err := range producer(ctx, headValue, remainingCursor) {
			// If a HoldForMappingComplete is yielded, we need to wait for the *mapper* to complete
			// *yielding* the previous chunk before yielding more chunks.
			if isHoldForMappingComplete(p) {
				if chunkIndex == 0 {
					// This is an error state: we cannot yield a HoldForMappingComplete
					// before yielding any chunks. We should return an error to the caller.
					select {
					case <-ctx.Done():
						return

					case orderedProducerChunks <- &producerChunkOrError[P, C, I]{err: spiceerrors.MustBugf("HoldForMappingComplete cannot be yielded before any chunks")}:
						return
					}
				}

				select {
				case <-ctx.Done():
					return

				case <-producerChunksByID[chunkIndex-1].wereMappingsYieldedChan:
					continue
				}
			}

			chunk := &producerChunkOrError[P, C, I]{
				chunkIndex:     chunkIndex,
				chunkAndFollow: p.(ChunkAndFollow[P, C]),
				err:            err,

				mappedResultChan:        make(chan mapperResultOrError[I, C], 1),
				wereMappingsYieldedChan: make(chan bool, 1),
			}
			producerChunksByID[chunkIndex] = chunk

			select {
			case <-ctx.Done():
				return

			case orderedProducerChunks <- chunk:
				// Continue processing chunks.
			}

			// Schedule the mapper task for this chunk
			chunkRemainingCursor := remainingCursor
			if chunkIndex > 0 {
				chunkRemainingCursor = nil
			}
			tr.Schedule(mapperTaskFunc(chunk, chunkRemainingCursor))

			chunkIndex++

			// If an error occurred, we sent it to the channel and now we stop producing further chunks.
			if err != nil {
				return
			}
		}

		// If we reach here, the producer has completed yielding chunks.
		// We need to signal that the last chunk has been completed.
		chunk := &producerChunkOrError[P, C, I]{
			chunkIndex: chunkIndex,
			completed:  true,

			mappedResultChan:        make(chan mapperResultOrError[I, C], 1),
			wereMappingsYieldedChan: make(chan bool, 1),
		}
		producerChunksByID[chunkIndex] = chunk

		select {
		case <-ctx.Done():
			return

		case orderedProducerChunks <- chunk:
			// Sent
		}

		// No need to schedule a mapper task for the completion chunk
	}

	return func(yield func(ItemAndCursor[I], error) bool) {
		defer cancel()

		// Start the producer in a goroutine.
		go producerFunc()

		for {
			select {
			case <-ctx.Done():
				return

			case chunk, ok := <-orderedProducerChunks:
				if !ok {
					return
				}

				if chunk.completed {
					return
				}

				if chunk.err != nil {
					_ = yield(ItemAndCursor[I]{}, chunk.err)
					return
				}

				select {
				case <-ctx.Done():
					return

				case mappedResult := <-chunk.mappedResultChan:
					if mappedResult.err != nil {
						_ = yield(ItemAndCursor[I]{}, mappedResult.err)
						return
					}

					cursorStr, err := cursorToStringConverter(chunk.chunkAndFollow.Follow)
					if err != nil {
						_ = yield(ItemAndCursor[I]{}, err)
						return
					}

					for _, r := range mappedResult.items {
						if !yield(ItemAndCursor[I]{
							Item:   r.Item,
							Cursor: r.Cursor.withPrefix(cursorStr),
						}, nil) {
							return
						}
					}

					select {
					case <-ctx.Done():
						return

					case chunk.wereMappingsYieldedChan <- true:
						// We have yielded all mappings for this chunk, we can continue processing
						// the next chunk.
					}
				}
			}
		}
	}
}
