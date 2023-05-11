package graph

import (
	"context"
	"strconv"
	"testing"

	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestCursorWithWrongRevision(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	require.Panics(t, func() {
		newCursorInformation(&v1.Cursor{}, revision, limits)
	})
}

func TestCursorHasHeadSectionOnEmpty(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	ci, err := newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{},
	}, revision, limits)
	require.NoError(t, err)

	hasFirst, err := ci.hasHeadSection("first")
	require.False(t, hasFirst)
	require.NoError(t, err)
}

func TestCursorSections(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	ci, err := newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"first", "1", "second", "two"},
	}, revision, limits)
	require.NoError(t, err)

	hasFirst, err := ci.hasHeadSection("first")
	require.True(t, hasFirst)
	require.NoError(t, err)

	value, err := ci.sectionValue("first")
	require.NoError(t, err)
	require.Equal(t, value, "1")

	ivalue, err := ci.integerSectionValue("first")
	require.NoError(t, err)
	require.Equal(t, ivalue, 1)
}

func TestCursorNonIntSection(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	ci, err := newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"first", "one", "second", "two"},
	}, revision, limits)
	require.NoError(t, err)

	hasFirst, err := ci.hasHeadSection("first")
	require.True(t, hasFirst)
	require.NoError(t, err)

	value, err := ci.sectionValue("first")
	require.NoError(t, err)
	require.Equal(t, value, "one")

	_, err = ci.integerSectionValue("first")
	require.Error(t, err)
}

func TestWithIterableInCursor(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	ci, err := newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{},
	}, revision, limits)
	require.NoError(t, err)

	i := 0
	items := []string{"one", "two", "three", "four"}
	err = withIterableInCursor(ci, "iter", items,
		func(cc cursorInformation, item string) error {
			require.Equal(t, items[i], item)
			require.Equal(t, []string{"iter", strconv.Itoa(i)}, cc.outgoingCursorSections)
			i++
			return nil
		})

	require.NoError(t, err)
	require.Equal(t, 4, i)

	ci, err = newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"iter", "3"},
	}, revision, limits)
	require.NoError(t, err)

	j := 3
	err = withIterableInCursor(ci, "iter", items,
		func(cc cursorInformation, item string) error {
			require.Equal(t, items[j], item)
			require.Equal(t, []string{"iter", strconv.Itoa(j)}, cc.outgoingCursorSections)
			j++
			return nil
		})

	require.NoError(t, err)
}

func TestWithDatastoreCursorInCursor(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	ci, err := newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"dsc", "document:firstdoc#viewer@user:tom"},
	}, revision, limits)
	require.NoError(t, err)

	i := 0
	cursors := []string{
		"document:firstdoc#viewer@user:tom",
		"document:seconddoc#viewer@user:tom",
		"document:thirddoc#viewer@user:tom",
	}

	err = withDatastoreCursorInCursor(ci, "dsc",
		func(queryCursor options.Cursor, ci cursorInformation) (options.Cursor, error) {
			require.Equal(t, cursors[i], tuple.MustString(queryCursor))
			i++
			if i >= len(cursors) {
				return nil, nil
			}

			return options.Cursor(tuple.MustParse(cursors[i])), nil
		})
	require.NoError(t, err)
	require.Equal(t, i, 3)
}

func TestWithSubsetInCursor(t *testing.T) {
	limits, _ := newLimitTracker(context.Background(), 10)
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))

	ci, err := newCursorInformation(&v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"current", "100"},
	}, revision, limits)
	require.NoError(t, err)

	handlerCalled := false
	nextCalled := false
	err = withSubsetInCursor(ci, "current",
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			require.Equal(t, 100, currentOffset)
			handlerCalled = true
			return nil
		},
		func(c cursorInformation) error {
			nextCalled = true
			return nil
		})
	require.NoError(t, err)
	require.True(t, handlerCalled)
	require.True(t, nextCalled)
}

func TestMustCombineCursors(t *testing.T) {
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))
	cursor1 := &v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"a", "b", "c"},
	}
	cursor2 := &v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"d", "e", "f"},
	}

	combined := mustCombineCursors(cursor1, cursor2)
	require.Equal(t, combined.AtRevision, revision.String())
	require.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, combined.Sections)
}

func TestMustCombineCursorsWithNil(t *testing.T) {
	revision := revision.NewFromDecimal(decimal.NewFromInt(1))
	cursor2 := &v1.Cursor{
		AtRevision: revision.String(),
		Sections:   []string{"d", "e", "f"},
	}

	combined := mustCombineCursors(nil, cursor2)
	require.Equal(t, combined.AtRevision, revision.String())
	require.Equal(t, []string{"d", "e", "f"}, combined.Sections)
}
