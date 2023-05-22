package graph

import (
	"strconv"

	"golang.org/x/exp/slices"

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

// mustWithOutgoingSection returns cursorInformation updated with the given name and optional
// value(s) appended to the outgoingCursorSections for the current cursor. If the current
// cursor already begins with the given name, its value is replaced.
func (ci cursorInformation) mustWithOutgoingSection(name string, values ...string) cursorInformation {
	hasSection, err := ci.hasHeadSection(name)
	if err != nil {
		panic(err)
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
		}
	}

	return cursorInformation{
		currentCursor:          nil,
		outgoingCursorSections: ocs,
		limits:                 ci.limits,
		revision:               ci.revision,
	}
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
		currentCursor := ci.mustWithOutgoingSection(name, strconv.Itoa(index))
		if !isFirstIteration {
			currentCursor = currentCursor.clearIncoming()
		}

		err := handler(currentCursor, item)
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

		currentCursor := ci.mustWithOutgoingSection(name, tuple.MustString(datastoreCursor))
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
		err = handler(afterIndex, func(nextOffset int) *v1.Cursor {
			return ci.mustWithOutgoingSection(name, strconv.Itoa(nextOffset)).responsePartialCursor()
		})
		if err != nil {
			return err
		}
	}

	if ci.limits.hasExhaustedLimit() {
		return nil
	}

	// -1 means that the handler has been completed.
	return next(ci.mustWithOutgoingSection(name, "-1"))
}

// mustCombineCursors combines the given cursors into one resulting cursor.
func mustCombineCursors(cursor *v1.Cursor, toAdd *v1.Cursor) *v1.Cursor {
	if toAdd == nil {
		panic("toAdd cannot be nil")
	}

	if cursor == nil {
		return &v1.Cursor{
			AtRevision: toAdd.AtRevision,
			Sections:   toAdd.Sections,
		}
	}

	return &v1.Cursor{
		AtRevision: cursor.AtRevision,
		Sections:   append(slices.Clone(cursor.Sections), toAdd.Sections...),
	}
}
