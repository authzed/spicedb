package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore/options"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestCursorProduction(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 42,
		Sections:        []string{"1", "2", "3"},
	}, limits, 42)
	require.NoError(t, err)

	cursor := ci.responsePartialCursor()
	require.Equal(t, uint32(42), cursor.DispatchVersion)
	require.Empty(t, cursor.Sections)

	cci, err := ci.withOutgoingSection("4")
	require.NoError(t, err)

	ccursor := cci.responsePartialCursor()

	require.Equal(t, uint32(42), ccursor.DispatchVersion)
	require.Equal(t, []string{"4"}, ccursor.Sections)
}

func TestCursorDifferentDispatchVersion(t *testing.T) {
	limits := newLimitTracker(10)

	_, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 2,
		Sections:        []string{},
	}, limits, 1)
	require.Error(t, err)
}

func TestCursorHasHeadSectionOnEmpty(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{},
	}, limits, 1)
	require.NoError(t, err)

	value, ok := ci.headSectionValue()
	require.False(t, ok)
	require.Equal(t, "", value)
}

func TestCursorWithClonedLimits(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{},
	}, limits, 1)
	require.NoError(t, err)

	require.Equal(t, uint32(10), ci.limits.currentLimit)
	require.Equal(t, uint32(1), ci.dispatchCursorVersion)

	cloned := ci.withClonedLimits()
	require.Equal(t, uint32(10), cloned.limits.currentLimit)
	require.Equal(t, uint32(1), cloned.dispatchCursorVersion)

	require.True(t, limits.prepareForPublishing())

	require.Equal(t, uint32(9), ci.limits.currentLimit)
	require.Equal(t, uint32(1), ci.dispatchCursorVersion)

	require.Equal(t, uint32(10), cloned.limits.currentLimit)
	require.Equal(t, uint32(1), cloned.dispatchCursorVersion)
}

func TestCursorSections(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"1", "two"},
	}, limits, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), ci.dispatchCursorVersion)

	value, ok := ci.headSectionValue()
	require.True(t, ok)
	require.Equal(t, value, "1")

	ivalue, err := ci.integerSectionValue()
	require.NoError(t, err)
	require.Equal(t, ivalue, 1)
}

func TestCursorNonIntSection(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"one", "two"},
	}, limits, 1)
	require.NoError(t, err)

	value, ok := ci.headSectionValue()
	require.True(t, ok)
	require.Equal(t, value, "one")

	_, err = ci.integerSectionValue()
	require.Error(t, err)
}

func TestWithSubsetInCursor(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"100"},
	}, limits, 1)
	require.NoError(t, err)

	handlerCalled := false
	nextCalled := false
	err = withSubsetInCursor(ci,
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

func TestCombineCursors(t *testing.T) {
	cursor1 := &v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"a", "b", "c"},
	}
	cursor2 := &v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"d", "e", "f"},
	}

	combined, err := combineCursors(cursor1, cursor2)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, combined.Sections)
}

func TestCombineCursorsWithNil(t *testing.T) {
	cursor2 := &v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"d", "e", "f"},
	}

	combined, err := combineCursors(nil, cursor2)
	require.NoError(t, err)
	require.Equal(t, []string{"d", "e", "f"}, combined.Sections)
}

func TestWithParallelizedStreamingIterableInCursor(t *testing.T) {
	limits := newLimitTracker(50)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{},
	}, limits, 1)
	require.NoError(t, err)

	items := []int{10, 20, 30, 40, 50}
	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		items,
		parentStream,
		2,
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			err := stream.Publish(item * 10)
			if err != nil {
				return err
			}

			return stream.Publish((item * 10) + 1)
		})

	require.NoError(t, err)
	require.Equal(t, []int{100, 101, 200, 201, 300, 301, 400, 401, 500, 501}, parentStream.Results())
}

func TestWithParallelizedStreamingIterableInCursorWithExistingCursor(t *testing.T) {
	limits := newLimitTracker(50)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"2"},
	}, limits, 1)
	require.NoError(t, err)

	items := []int{10, 20, 30, 40, 50}
	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		items,
		parentStream,
		2,
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			err := stream.Publish(item * 10)
			if err != nil {
				return err
			}

			return stream.Publish((item * 10) + 1)
		})

	require.NoError(t, err)
	require.Equal(t, []int{300, 301, 400, 401, 500, 501}, parentStream.Results())
}

func TestWithParallelizedStreamingIterableInCursorWithLimit(t *testing.T) {
	limits := newLimitTracker(5)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{},
	}, limits, 1)
	require.NoError(t, err)

	items := []int{10, 20, 30, 40, 50}
	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		items,
		parentStream,
		2,
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			err := stream.Publish(item * 10)
			if err != nil {
				return err
			}

			return stream.Publish((item * 10) + 1)
		})

	require.NoError(t, err)
	require.Equal(t, []int{100, 101, 200, 201, 300}, parentStream.Results())
}

func TestWithParallelizedStreamingIterableInCursorEnsureParallelism(t *testing.T) {
	limits := newLimitTracker(500)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{},
	}, limits, 1)
	require.NoError(t, err)

	items := []int{}
	expected := []int{}
	for i := 0; i < 500; i++ {
		items = append(items, i)
		expected = append(expected, i*10)
	}

	encountered := []int{}
	lock := sync.Mutex{}

	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		items,
		parentStream,
		5,
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			lock.Lock()
			encountered = append(encountered, item)
			lock.Unlock()

			return stream.Publish(item * 10)
		})

	require.Equal(t, len(expected), len(encountered))
	require.NotEqual(t, encountered, expected)

	require.NoError(t, err)
	require.Equal(t, expected, parentStream.Results())
}

func TestWithDatastoreCursorInCursor(t *testing.T) {
	limits := newLimitTracker(500)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{},
	}, limits, 1)
	require.NoError(t, err)

	encountered := []int{}
	lock := sync.Mutex{}

	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withDatastoreCursorInCursor[int, int](
		context.Background(),
		ci,
		parentStream,
		5,
		func(queryCursor options.Cursor) ([]itemAndPostCursor[int], error) {
			return []itemAndPostCursor[int]{
				{1, options.ToCursor(tuple.MustParse("document:foo#viewer@user:tom"))},
				{2, options.ToCursor(tuple.MustParse("document:foo#viewer@user:sarah"))},
				{3, options.ToCursor(tuple.MustParse("document:foo#viewer@user:fred"))},
			}, nil
		},
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			lock.Lock()
			encountered = append(encountered, item)
			lock.Unlock()

			return stream.Publish(item * 10)
		})

	expected := []int{10, 20, 30}

	require.Equal(t, len(expected), len(encountered))
	require.NotEqual(t, encountered, expected)

	require.NoError(t, err)
	require.Equal(t, expected, parentStream.Results())
}

func TestWithDatastoreCursorInCursorWithStartingCursor(t *testing.T) {
	limits := newLimitTracker(500)

	ci, err := newCursorInformation(&v1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{"", "42"},
	}, limits, 1)
	require.NoError(t, err)

	encountered := []int{}
	lock := sync.Mutex{}

	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withDatastoreCursorInCursor(
		context.Background(),
		ci,
		parentStream,
		5,
		func(queryCursor options.Cursor) ([]itemAndPostCursor[int], error) {
			require.Nil(t, queryCursor)

			return []itemAndPostCursor[int]{
				{2, options.ToCursor(tuple.MustParse("document:foo#viewer@user:sarah"))},
				{3, options.ToCursor(tuple.MustParse("document:foo#viewer@user:fred"))},
			}, nil
		},
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			lock.Lock()
			encountered = append(encountered, item)
			lock.Unlock()

			if v, _ := cc.headSectionValue(); v != "" {
				value, _ := cc.integerSectionValue()
				item = item + value
			}

			return stream.Publish(item * 10)
		})

	require.NoError(t, err)

	expected := []int{440, 30}
	require.Equal(t, len(expected), len(encountered))
	require.Equal(t, expected, parentStream.Results())
}
