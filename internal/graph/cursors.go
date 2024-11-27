package graph

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/datastore/options"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// cursorInformation is a struct which holds information about the current incoming cursor (if any)
// and the sections to be added to the *outgoing* partial cursor.
type cursorInformation struct {
	// currentCursor is the current incoming cursor. This may be nil.
	currentCursor *v1.Cursor

	// outgoingCursorSections are the sections to be added to the outgoing *partial* cursor.
	// It is the responsibility of the *caller* to append together the incoming cursors to form
	// the final cursor.
	//
	// A `section` is a portion of the cursor, representing a section of code that was
	// executed to produce the section of the cursor.
	outgoingCursorSections []string

	// limits is the limits tracker for the call over which the cursor is being used.
	limits *limitTracker

	// dispatchCursorVersion is the version of the dispatch to be stored in the cursor.
	dispatchCursorVersion uint32
}

// newCursorInformation constructs a new cursorInformation struct from the incoming cursor (which
// may be nil)
func newCursorInformation(incomingCursor *v1.Cursor, limits *limitTracker, dispatchCursorVersion uint32) (cursorInformation, error) {
	if incomingCursor != nil && incomingCursor.DispatchVersion != dispatchCursorVersion {
		return cursorInformation{}, NewInvalidCursorErr(dispatchCursorVersion, incomingCursor)
	}

	if dispatchCursorVersion == 0 {
		return cursorInformation{}, spiceerrors.MustBugf("invalid dispatch cursor version")
	}

	return cursorInformation{
		currentCursor:          incomingCursor,
		outgoingCursorSections: nil,
		limits:                 limits,
		dispatchCursorVersion:  dispatchCursorVersion,
	}, nil
}

// responsePartialCursor is the *partial* cursor to return in a response.
func (ci cursorInformation) responsePartialCursor() *v1.Cursor {
	return &v1.Cursor{
		DispatchVersion: ci.dispatchCursorVersion,
		Sections:        ci.outgoingCursorSections,
	}
}

// withClonedLimits returns the cursor, but with its limits tracker cloned.
func (ci cursorInformation) withClonedLimits() cursorInformation {
	return cursorInformation{
		currentCursor:          ci.currentCursor,
		outgoingCursorSections: ci.outgoingCursorSections,
		limits:                 ci.limits.clone(),
		dispatchCursorVersion:  ci.dispatchCursorVersion,
	}
}

// headSectionValue returns the string value found at the head of the incoming cursor.
// If the incoming cursor is empty, returns empty.
func (ci cursorInformation) headSectionValue() (string, bool) {
	if ci.currentCursor == nil || len(ci.currentCursor.Sections) < 1 {
		return "", false
	}

	return ci.currentCursor.Sections[0], true
}

// integerSectionValue returns the *integer* found  at the head of the incoming cursor.
// If the incoming cursor is empty, returns 0. If the incoming cursor does not start with an
// int value, fails with an error.
func (ci cursorInformation) integerSectionValue() (int, error) {
	valueStr, hasValue := ci.headSectionValue()
	if !hasValue {
		return 0, nil
	}

	if valueStr == "" {
		return 0, nil
	}

	return strconv.Atoi(valueStr)
}

// withOutgoingSection returns cursorInformation updated with the given optional
// value appended to the outgoingCursorSections for the current cursor. If the current
// cursor already begins with any values, those values are replaced.
func (ci cursorInformation) withOutgoingSection(value string) (cursorInformation, error) {
	ocs := make([]string, 0, len(ci.outgoingCursorSections)+1)
	ocs = append(ocs, ci.outgoingCursorSections...)
	ocs = append(ocs, value)

	if ci.currentCursor != nil && len(ci.currentCursor.Sections) > 0 {
		// If the cursor already has values, replace them with those specified.
		return cursorInformation{
			currentCursor: &v1.Cursor{
				DispatchVersion: ci.dispatchCursorVersion,
				Sections:        ci.currentCursor.Sections[1:],
			},
			outgoingCursorSections: ocs,
			limits:                 ci.limits,
			dispatchCursorVersion:  ci.dispatchCursorVersion,
		}, nil
	}

	return cursorInformation{
		currentCursor:          nil,
		outgoingCursorSections: ocs,
		limits:                 ci.limits,
		dispatchCursorVersion:  ci.dispatchCursorVersion,
	}, nil
}

func (ci cursorInformation) clearIncoming() cursorInformation {
	return cursorInformation{
		currentCursor:          nil,
		outgoingCursorSections: ci.outgoingCursorSections,
		limits:                 ci.limits,
		dispatchCursorVersion:  ci.dispatchCursorVersion,
	}
}

type cursorHandler func(c cursorInformation) error

// itemAndPostCursor represents an item and the cursor to be used for all items after it.
type itemAndPostCursor[T any] struct {
	item   T
	cursor options.Cursor
}

// withDatastoreCursorInCursor executes the given lookup function to retrieve items from the datastore,
// and then executes the handler on each of the produced items *in parallel*, streaming the results
// in the correct order to the parent stream.
func withDatastoreCursorInCursor[T any, Q any](
	ctx context.Context,
	ci cursorInformation,
	parentStream dispatch.Stream[Q],
	concurrencyLimit uint16,
	lookup func(queryCursor options.Cursor) ([]itemAndPostCursor[T], error),
	handler func(ctx context.Context, ci cursorInformation, item T, stream dispatch.Stream[Q]) error,
) error {
	// Retrieve the *datastore* cursor, if one is found at the head of the incoming cursor.
	var datastoreCursor options.Cursor
	datastoreCursorString, _ := ci.headSectionValue()
	if datastoreCursorString != "" {
		datastoreCursor = options.ToCursor(tuple.MustParse(datastoreCursorString))
	}

	if ci.limits.hasExhaustedLimit() {
		return nil
	}

	// Execute the lookup to call the database and find items for processing.
	itemsToBeProcessed, err := lookup(datastoreCursor)
	if err != nil {
		return err
	}

	if len(itemsToBeProcessed) == 0 {
		return nil
	}

	itemsToRun := make([]T, 0, len(itemsToBeProcessed))
	for _, itemAndCursor := range itemsToBeProcessed {
		itemsToRun = append(itemsToRun, itemAndCursor.item)
	}

	getItemCursor := func(taskIndex int) (cursorInformation, error) {
		// Create an updated cursor referencing the current item's cursor, so that any items returned know to resume from this point.
		cursorRel := options.ToRelationship(itemsToBeProcessed[taskIndex].cursor)
		cursorSection := ""
		if cursorRel != nil {
			cursorSection = tuple.StringWithoutCaveatOrExpiration(*cursorRel)
		}

		currentCursor, err := ci.withOutgoingSection(cursorSection)
		if err != nil {
			return currentCursor, err
		}

		// If not the first iteration, we need to clear incoming sections to ensure the iteration starts at the top
		// of the cursor.
		if taskIndex > 0 {
			currentCursor = currentCursor.clearIncoming()
		}

		return currentCursor, nil
	}

	return withInternalParallelizedStreamingIterableInCursor(
		ctx,
		ci,
		itemsToRun,
		parentStream,
		concurrencyLimit,
		getItemCursor,
		handler,
	)
}

type afterResponseCursor func(nextOffset int) *v1.Cursor

// withSubsetInCursor executes the given handler with the offset index found at the beginning of the
// cursor. If the offset is not found, executes with 0. The handler is given the current offset as
// well as a callback to mint the cursor with the next offset.
func withSubsetInCursor(
	ci cursorInformation,
	handler func(currentOffset int, nextCursorWith afterResponseCursor) error,
	next cursorHandler,
) error {
	if ci.limits.hasExhaustedLimit() {
		return nil
	}

	afterIndex, err := ci.integerSectionValue()
	if err != nil {
		return err
	}

	if afterIndex >= 0 {
		var foundCerr error
		err = handler(afterIndex, func(nextOffset int) *v1.Cursor {
			cursor, cerr := ci.withOutgoingSection(strconv.Itoa(nextOffset))
			foundCerr = cerr
			if cerr != nil {
				return nil
			}

			return cursor.responsePartialCursor()
		})
		if err != nil {
			return err
		}
		if foundCerr != nil {
			return foundCerr
		}
	}

	if ci.limits.hasExhaustedLimit() {
		return nil
	}

	// -1 means that the handler has been completed.
	uci, err := ci.withOutgoingSection("-1")
	if err != nil {
		return err
	}
	return next(uci)
}

// combineCursors combines the given cursors into one resulting cursor.
func combineCursors(cursor *v1.Cursor, toAdd *v1.Cursor) (*v1.Cursor, error) {
	if toAdd == nil || len(toAdd.Sections) == 0 {
		return nil, spiceerrors.MustBugf("supplied toAdd cursor was nil or empty")
	}

	if cursor == nil || len(cursor.Sections) == 0 {
		return toAdd, nil
	}

	sections := make([]string, 0, len(cursor.Sections)+len(toAdd.Sections))
	sections = append(sections, cursor.Sections...)
	sections = append(sections, toAdd.Sections...)

	return &v1.Cursor{
		DispatchVersion: toAdd.DispatchVersion,
		Sections:        sections,
	}, nil
}

// withParallelizedStreamingIterableInCursor executes the given handler for each item in the items list, skipping any
// items marked as completed at the head of the cursor and injecting a cursor representing the current
// item.
//
// For example, if items contains 3 items, and the cursor returned was within the handler for item
// index #1, then item index #0 will be skipped on subsequent invocation.
//
// The next index is executed in parallel with the current index, with its results stored in a CollectingStream
// until the next iteration.
func withParallelizedStreamingIterableInCursor[T any, Q any](
	ctx context.Context,
	ci cursorInformation,
	items []T,
	parentStream dispatch.Stream[Q],
	concurrencyLimit uint16,
	handler func(ctx context.Context, ci cursorInformation, item T, stream dispatch.Stream[Q]) error,
) error {
	// Check the cursor for a starting index, before which any items will be skipped.
	startingIndex, err := ci.integerSectionValue()
	if err != nil {
		return err
	}

	if startingIndex < 0 || startingIndex > len(items) {
		return spiceerrors.MustBugf("invalid cursor in withParallelizedStreamingIterableInCursor: found starting index %d for items %v", startingIndex, items)
	}

	itemsToRun := items[startingIndex:]
	if len(itemsToRun) == 0 {
		return nil
	}

	getItemCursor := func(taskIndex int) (cursorInformation, error) {
		// Create an updated cursor referencing the current item's index, so that any items returned know to resume from this point.
		currentCursor, err := ci.withOutgoingSection(strconv.Itoa(taskIndex + startingIndex))
		if err != nil {
			return currentCursor, err
		}

		// If not the first iteration, we need to clear incoming sections to ensure the iteration starts at the top
		// of the cursor.
		if taskIndex > 0 {
			currentCursor = currentCursor.clearIncoming()
		}

		return currentCursor, nil
	}

	return withInternalParallelizedStreamingIterableInCursor(
		ctx,
		ci,
		itemsToRun,
		parentStream,
		concurrencyLimit,
		getItemCursor,
		handler,
	)
}

func withInternalParallelizedStreamingIterableInCursor[T any, Q any](
	ctx context.Context,
	ci cursorInformation,
	itemsToRun []T,
	parentStream dispatch.Stream[Q],
	concurrencyLimit uint16,
	getItemCursor func(taskIndex int) (cursorInformation, error),
	handler func(ctx context.Context, ci cursorInformation, item T, stream dispatch.Stream[Q]) error,
) error {
	// Queue up each iteration's worth of items to be run by the task runner.
	tr := taskrunner.NewPreloadedTaskRunner(ctx, concurrencyLimit, len(itemsToRun))
	stream, err := newParallelLimitedIndexedStream(ctx, ci, parentStream, len(itemsToRun))
	if err != nil {
		return err
	}

	// Schedule a task to be invoked for each item to be run.
	for taskIndex, item := range itemsToRun {
		taskIndex := taskIndex
		item := item
		tr.Add(func(ctx context.Context) error {
			stream.lock.Lock()
			if ci.limits.hasExhaustedLimit() {
				stream.lock.Unlock()
				return nil
			}
			stream.lock.Unlock()

			ici, err := getItemCursor(taskIndex)
			if err != nil {
				return err
			}

			// Invoke the handler with the current item's index in the outgoing cursor, indicating that
			// subsequent invocations should jump right to this item.
			ictx, istream, icursor := stream.forTaskIndex(ctx, taskIndex, ici)

			err = handler(ictx, icursor, item, istream)
			if err != nil {
				// If the branch was canceled explicitly by *this* streaming iterable because other branches have fulfilled
				// the configured limit, then we can safely ignore this error.
				if errors.Is(context.Cause(ictx), stream.errCanceledBecauseFulfilled) {
					return nil
				}
				return err
			}

			return stream.completedTaskIndex(taskIndex)
		})
	}

	err = tr.StartAndWait()
	if err != nil {
		return err
	}
	return nil
}

// parallelLimitedIndexedStream is a specialization of a dispatch.Stream that collects results from multiple
// tasks running in parallel, and emits them in the order of the tasks. The first task's results are directly
// emitted to the parent stream, while subsequent tasks' results are emitted in the defined order of the tasks
// to ensure cursors and limits work as expected.
type parallelLimitedIndexedStream[Q any] struct {
	lock sync.Mutex

	ctx          context.Context
	ci           cursorInformation
	parentStream dispatch.Stream[Q]

	streamCount                 int
	toPublishTaskIndex          int
	countingStream              *dispatch.CountingDispatchStream[Q]
	childStreams                map[int]*dispatch.CollectingDispatchStream[Q]
	childContextCancels         map[int]func(cause error)
	completedTaskIndexes        map[int]bool
	errCanceledBecauseFulfilled error
}

func newParallelLimitedIndexedStream[Q any](
	ctx context.Context,
	ci cursorInformation,
	parentStream dispatch.Stream[Q],
	streamCount int,
) (*parallelLimitedIndexedStream[Q], error) {
	if streamCount <= 0 {
		return nil, spiceerrors.MustBugf("got invalid stream count")
	}

	return &parallelLimitedIndexedStream[Q]{
		ctx:                  ctx,
		ci:                   ci,
		parentStream:         parentStream,
		countingStream:       nil,
		childStreams:         map[int]*dispatch.CollectingDispatchStream[Q]{},
		childContextCancels:  map[int]func(cause error){},
		completedTaskIndexes: map[int]bool{},
		toPublishTaskIndex:   0,
		streamCount:          streamCount,

		// NOTE: we mint a new error here to ensure that we only skip cancelations from this very instance.
		errCanceledBecauseFulfilled: errors.New("canceled because other branches fulfilled limit"),
	}, nil
}

// forTaskIndex returns a new context, stream and cursor for invoking the task at the specific index and publishing its results.
func (ls *parallelLimitedIndexedStream[Q]) forTaskIndex(ctx context.Context, index int, currentCursor cursorInformation) (context.Context, dispatch.Stream[Q], cursorInformation) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	// Create a new cursor with cloned limits, because each child task which executes (in parallel) will need its own
	// limit tracking. The overall limit on the original cursor is managed in completedTaskIndex.
	childCI := currentCursor.withClonedLimits()
	childContext, cancelDispatch := branchContext(ctx)

	ls.childContextCancels[index] = cancelDispatch

	// If executing for the first index, it can stream directly to the parent stream, but we need to count the number
	// of items streamed to adjust the overall limits.
	if index == 0 {
		countingStream := dispatch.NewCountingDispatchStream(ls.parentStream)
		ls.countingStream = countingStream
		return childContext, countingStream, childCI
	}

	// Otherwise, create a child stream with an adjusted limits on the cursor. We have to clone the cursor's
	// limits here to ensure that the child's publishing doesn't affect the first branch.
	childStream := dispatch.NewCollectingDispatchStream[Q](childContext)
	ls.childStreams[index] = childStream

	return childContext, childStream, childCI
}

// cancelRemainingDispatches cancels the contexts for each dispatched branch, indicating that no additional results
// are necessary.
func (ls *parallelLimitedIndexedStream[Q]) cancelRemainingDispatches() {
	for _, cancel := range ls.childContextCancels {
		cancel(ls.errCanceledBecauseFulfilled)
	}
}

// completedTaskIndex indicates the the task at the specific index has completed successfully and that its collected
// results should be published to the parent stream, so long as all previous tasks have been completed and published as well.
func (ls *parallelLimitedIndexedStream[Q]) completedTaskIndex(index int) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	// Mark the task as completed, but not yet published.
	ls.completedTaskIndexes[index] = true

	// If the overall limit has been reached, nothing more to do.
	if ls.ci.limits.hasExhaustedLimit() {
		ls.cancelRemainingDispatches()
		return nil
	}

	// Otherwise, publish any results from previous completed tasks up, and including, this task. This loop ensures
	// that the collected results for each task are published to the parent stream in the correct order.
	for {
		if !ls.completedTaskIndexes[ls.toPublishTaskIndex] {
			return nil
		}

		if ls.toPublishTaskIndex == 0 {
			// Remove the already emitted data from the overall limits.
			publishedCount, err := safecast.ToUint32(ls.countingStream.PublishedCount())
			if err != nil {
				return spiceerrors.MustBugf("cannot cast published count to uint32: %v", err)
			}
			if err := ls.ci.limits.markAlreadyPublished(publishedCount); err != nil {
				return err
			}

			if ls.ci.limits.hasExhaustedLimit() {
				ls.cancelRemainingDispatches()
			}
		} else {
			// Publish, to the parent stream, the results produced by the task and stored in the child stream.
			childStream := ls.childStreams[ls.toPublishTaskIndex]
			for _, result := range childStream.Results() {
				if !ls.ci.limits.prepareForPublishing() {
					ls.cancelRemainingDispatches()
					return nil
				}

				err := ls.parentStream.Publish(result)
				if err != nil {
					return err
				}
			}
			ls.childStreams[ls.toPublishTaskIndex] = nil
		}

		ls.toPublishTaskIndex++
	}
}
