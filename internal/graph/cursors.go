package graph

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"golang.org/x/exp/slices"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
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
	// A `section` is a named portion of the cursor, representing a section of code that was
	// executed to produce the section of the cursor.
	outgoingCursorSections []string

	// limits is the limits tracker for the call over which the cursor is being used.
	limits *limitTracker

	// revision is the revision at which cursors are being created.
	revision datastore.Revision
}

// newCursorInformation constructs a new cursorInformation struct from the incoming cursor (which
// may be nil)
func newCursorInformation(incomingCursor *v1.Cursor, revision datastore.Revision, limits *limitTracker) (cursorInformation, error) {
	if incomingCursor != nil && incomingCursor.AtRevision != revision.String() {
		return cursorInformation{}, spiceerrors.MustBugf("revision mismatch when creating cursor information. expected: %s, found: %s", revision.String(), incomingCursor.AtRevision)
	}

	return cursorInformation{
		currentCursor:          incomingCursor,
		outgoingCursorSections: nil,
		limits:                 limits,
		revision:               revision,
	}, nil
}

// responsePartialCursor is the *partial* cursor to return in a response.
func (ci cursorInformation) responsePartialCursor() *v1.Cursor {
	return &v1.Cursor{
		AtRevision: ci.revision.String(),
		Sections:   ci.outgoingCursorSections,
	}
}

func (ci cursorInformation) withClonedLimits(ctx context.Context) (cursorInformation, context.Context) {
	cloned, ctx := ci.limits.clone(ctx)
	return cursorInformation{
		currentCursor:          ci.currentCursor,
		outgoingCursorSections: ci.outgoingCursorSections,
		limits:                 cloned,
		revision:               ci.revision,
	}, ctx
}

// hasHeadSection returns true if the current cursor has the given name as the prefix of the cursor.
func (ci cursorInformation) hasHeadSection(name string) (bool, error) {
	if ci.currentCursor == nil || len(ci.currentCursor.Sections) == 0 {
		return false, nil
	}

	if ci.currentCursor.Sections[0] != name {
		return false, spiceerrors.MustBugf("expected cursor section %s in %v", name, ci.currentCursor.Sections)
	}

	return true, nil
}

// sectionValue returns the string value found after the `name` at the head of the incoming cursor.
// If the incoming cursor is empty, returns empty. If the incoming cursor does not start with the name,
// fails with an error.
func (ci cursorInformation) sectionValue(name string) (string, error) {
	if ci.currentCursor == nil || len(ci.currentCursor.Sections) < 2 {
		return "", nil
	}

	if ci.currentCursor.Sections[0] != name {
		return "", spiceerrors.MustBugf("expected cursor section %s in %v", name, ci.currentCursor.Sections)
	}

	return ci.currentCursor.Sections[1], nil
}

// integerSectionValue returns the *integer* found after the `name` at the head of the incoming cursor.
// If the incoming cursor is empty, returns 0. If the incoming cursor does not start with the name,
// fails with an error.
func (ci cursorInformation) integerSectionValue(name string) (int, error) {
	valueStr, err := ci.sectionValue(name)
	if err != nil {
		return 0, err
	}
	if valueStr == "" {
		return 0, err
	}

	return strconv.Atoi(valueStr)
}

// withOutgoingSection returns cursorInformation updated with the given name and optional
// value(s) appended to the outgoingCursorSections for the current cursor. If the current
// cursor already begins with the given name, its value is replaced.
func (ci cursorInformation) withOutgoingSection(name string, values ...string) (cursorInformation, error) {
	hasSection, err := ci.hasHeadSection(name)
	if err != nil {
		return cursorInformation{}, spiceerrors.MustBugf("mismatch on expected head section of the cursor: %s", name)
	}

	ocs := slices.Clone(ci.outgoingCursorSections)
	ocs = append(ocs, name)
	ocs = append(ocs, values...)

	if hasSection {
		// If the section already exists, remove it and its values in the cursor.
		return cursorInformation{
			currentCursor: &v1.Cursor{
				AtRevision: ci.revision.String(),
				Sections:   slices.Clone(ci.currentCursor.Sections[len(values)+1:]),
			},
			outgoingCursorSections: ocs,
			limits:                 ci.limits,
			revision:               ci.revision,
		}, nil
	}

	return cursorInformation{
		currentCursor:          nil,
		outgoingCursorSections: ocs,
		limits:                 ci.limits,
		revision:               ci.revision,
	}, nil
}

func (ci cursorInformation) clearIncoming() cursorInformation {
	return cursorInformation{
		currentCursor:          nil,
		outgoingCursorSections: ci.outgoingCursorSections,
		limits:                 ci.limits,
		revision:               ci.revision,
	}
}

type cursorHandler func(c cursorInformation) error

// withIterableInCursor executes the given handler for each item in the items list, skipping any
// items marked as completed at the head of the cursor and injecting a cursor representing the current
// item.
//
// For example, if items contains 3 items, and the cursor returned was within the handler for item
// index #1, then item index #0 will be skipped on subsequent invocation.
func withIterableInCursor[T any](
	ci cursorInformation,
	name string,
	items []T,
	handler func(ci cursorInformation, item T) error,
) error {
	// Check the index for the section in the cursor. If found, we skip any items before that index.
	afterIndex, err := ci.integerSectionValue(name)
	if err != nil {
		return err
	}

	isFirstIteration := true
	for index, item := range items {
		if index < afterIndex {
			continue
		}

		if ci.limits.hasExhaustedLimit() {
			return nil
		}

		// Invoke the handler with the current item's index in the outgoing cursor, indicating that
		// subsequent invocations should jump right to this item.
		currentCursor, err := ci.withOutgoingSection(name, strconv.Itoa(index))
		if err != nil {
			return err
		}

		if !isFirstIteration {
			currentCursor = currentCursor.clearIncoming()
		}

		err = handler(currentCursor, item)
		if err != nil {
			return err
		}

		isFirstIteration = false
	}

	return nil
}

// withDatastoreCursorInCursor executes the given handler until it returns an empty "next" datastore cursor,
// starting at the datastore cursor found in the cursor information (if any).
func withDatastoreCursorInCursor(
	ci cursorInformation,
	name string,
	handler func(queryCursor options.Cursor, ci cursorInformation) (options.Cursor, error),
) error {
	// Retrieve the *datastore* cursor, if one is found at the head of the incoming cursor.
	var datastoreCursor options.Cursor
	datastoreCursorString, err := ci.sectionValue(name)
	if err != nil {
		return err
	}

	if datastoreCursorString != "" {
		datastoreCursor = tuple.MustParse(datastoreCursorString)
	}

	// Execute the loop, starting at the datastore's cursor (if any), until there is no additional
	// datastore cursor returned.
	isFirstIteration := true
	for {
		if ci.limits.hasExhaustedLimit() {
			return nil
		}

		currentCursor, err := ci.withOutgoingSection(name, tuple.MustString(datastoreCursor))
		if err != nil {
			return err
		}

		if !isFirstIteration {
			currentCursor = currentCursor.clearIncoming()
		}

		nextDCCursor, err := handler(datastoreCursor, currentCursor)
		if err != nil {
			return err
		}
		if nextDCCursor == nil {
			return nil
		}
		datastoreCursor = nextDCCursor
		isFirstIteration = false
	}
}

type afterResponseCursor func(nextOffset int) *v1.Cursor

// withSubsetInCursor executes the given handler with the offset index found at the beginning of the
// cursor. If the offset is not found, executes with 0. The handler is given the current offset as
// well as a callback to mint the cursor with the next offset.
func withSubsetInCursor(
	ci cursorInformation,
	name string,
	handler func(currentOffset int, nextCursorWith afterResponseCursor) error,
	next cursorHandler,
) error {
	if ci.limits.hasExhaustedLimit() {
		return nil
	}

	afterIndex, err := ci.integerSectionValue(name)
	if err != nil {
		return err
	}

	if afterIndex >= 0 {
		var foundCerr error
		err = handler(afterIndex, func(nextOffset int) *v1.Cursor {
			cursor, cerr := ci.withOutgoingSection(name, strconv.Itoa(nextOffset))
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
	uci, err := ci.withOutgoingSection(name, "-1")
	if err != nil {
		return err
	}
	return next(uci)
}

// combineCursors combines the given cursors into one resulting cursor.
func combineCursors(cursor *v1.Cursor, toAdd *v1.Cursor) (*v1.Cursor, error) {
	if toAdd == nil {
		return nil, spiceerrors.MustBugf("supplied toAdd cursor was nil")
	}

	if cursor == nil {
		return &v1.Cursor{
			AtRevision: toAdd.AtRevision,
			Sections:   toAdd.Sections,
		}, nil
	}

	return &v1.Cursor{
		AtRevision: cursor.AtRevision,
		Sections:   append(slices.Clone(cursor.Sections), toAdd.Sections...),
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
	name string,
	items []T,
	parentStream dispatch.Stream[Q],
	concurrencyLimit uint16,
	handler func(ctx context.Context, ci cursorInformation, item T, stream dispatch.Stream[Q]) error,
) error {
	// Check the cursor for a starting index, before which any items will be skipped.
	startingIndex, err := ci.integerSectionValue(name)
	if err != nil {
		return err
	}

	itemsToRun := items[startingIndex:]
	if len(itemsToRun) == 0 {
		return nil
	}

	// Queue up each iteration's worth of items to be run by the task runner.
	tr := newPreloadedTaskRunner(ctx, concurrencyLimit, len(itemsToRun))
	stream, err := newParallelLimitedIndexedStream[Q](ctx, ci, parentStream, len(itemsToRun))
	if err != nil {
		return err
	}

	// Schedule a task to be invoked for each item to be run.
	for taskIndex, item := range itemsToRun {
		taskIndex := taskIndex
		item := item
		tr.add(func(ctx context.Context) error {
			if ci.limits.hasExhaustedLimit() {
				return nil
			}

			// Create an updated cursor referencing the current item's index, so that any items returned know to resume from this point.
			currentCursor, err := ci.withOutgoingSection(name, strconv.Itoa(taskIndex+startingIndex))
			if err != nil {
				return err
			}

			// If not the first iteration, we need to clear incoming sections to ensure the iteration starts at the top
			// of the cursor.
			if taskIndex > 0 {
				currentCursor = currentCursor.clearIncoming()
			}

			// Invoke the handler with the current item's index in the outgoing cursor, indicating that
			// subsequent invocations should jump right to this item.
			ictx, istream, icursor := stream.forTaskIndex(ctx, taskIndex, currentCursor)

			err = handler(ictx, icursor, item, istream)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}

			return stream.completedTaskIndex(taskIndex)
		})
	}

	// NOTE: since branches can be canceled if they have reached limits, the context Canceled error is ignored here.
	err = tr.startAndWait()
	if err != nil && !errors.Is(err, context.Canceled) {
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

	streamCount          int
	toPublishTaskIndex   int
	countingStream       *dispatch.CountingDispatchStream[Q]
	childStreams         map[int]*dispatch.CollectingDispatchStream[Q]
	completedTaskIndexes map[int]bool
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
		completedTaskIndexes: map[int]bool{},
		toPublishTaskIndex:   0,
		streamCount:          streamCount,
	}, nil
}

// forTaskIndex returns a new context, stream and cursor for invoking the task at the specific index and publishing its results.
func (ls *parallelLimitedIndexedStream[Q]) forTaskIndex(ctx context.Context, index int, currentCursor cursorInformation) (context.Context, dispatch.Stream[Q], cursorInformation) {
	// Create a new cursor with cloned limits, because each child task which executes (in parallel) will need its own
	// limit tracking. The overall limit on the original cursor is managed in completedTaskIndex.
	childCI, cctx := currentCursor.withClonedLimits(ctx)

	// If executing for the first index, it can stream directly to the parent stream, but we need to count the number
	// of items streamed to adjust the overall limits.
	if index == 0 {
		countingStream := dispatch.NewCountingDispatchStream[Q](ls.parentStream)
		ls.countingStream = countingStream
		return cctx, countingStream, childCI
	}

	// Otherwise, create a child stream with an adjusted limits on the cursor. We have to clone the cursor's
	// limits here to ensure that the child's publishing doesn't affect the first branch.
	ls.lock.Lock()
	defer ls.lock.Unlock()

	childStream := dispatch.NewCollectingDispatchStream[Q](ctx)
	ls.childStreams[index] = childStream
	return cctx, childStream, childCI
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
			done, err := ls.ci.limits.markAlreadyPublished(uint32(ls.countingStream.PublishedCount()))
			defer done()
			if err != nil {
				return err
			}
		} else {
			// Publish, to the parent stream, the results produced by the task and stored in the child stream.
			childStream := ls.childStreams[ls.toPublishTaskIndex]
			for _, result := range childStream.Results() {
				ok, done := ls.ci.limits.prepareForPublishing()
				defer done()

				if !ok {
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
