package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/authzed/spicedb/pkg/datastore/options"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestCursorHasHeadSectionOnEmpty(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		Sections: []string{},
	}, limits)
	require.NoError(t, err)

	hasFirst, err := ci.hasHeadSection("first")
	require.False(t, hasFirst)
	require.NoError(t, err)
}

func TestCursorSections(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		Sections: []string{"first", "1", "second", "two"},
	}, limits)
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
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		Sections: []string{"first", "one", "second", "two"},
	}, limits)
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

func TestWithSubsetInCursor(t *testing.T) {
	limits := newLimitTracker(10)

	ci, err := newCursorInformation(&v1.Cursor{
		Sections: []string{"current", "100"},
	}, limits)
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

func TestCombineCursors(t *testing.T) {

	cursor1 := &v1.Cursor{
		Sections: []string{"a", "b", "c"},
	}
	cursor2 := &v1.Cursor{
		Sections: []string{"d", "e", "f"},
	}

	combined, err := combineCursors(cursor1, cursor2)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, combined.Sections)
}

func TestCombineCursorsWithNil(t *testing.T) {
	cursor2 := &v1.Cursor{
		Sections: []string{"d", "e", "f"},
	}

	combined, err := combineCursors(nil, cursor2)
	require.NoError(t, err)
	require.Equal(t, []string{"d", "e", "f"}, combined.Sections)
}

func TestWithParallelizedStreamingIterableInCursor(t *testing.T) {
	limits := newLimitTracker(50)

	ci, err := newCursorInformation(&v1.Cursor{

		Sections: []string{},
	}, limits)
	require.NoError(t, err)

	items := []int{10, 20, 30, 40, 50}
	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		"iter",
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

		Sections: []string{"iter", "2"},
	}, limits)
	require.NoError(t, err)

	items := []int{10, 20, 30, 40, 50}
	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		"iter",
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

		Sections: []string{},
	}, limits)
	require.NoError(t, err)

	items := []int{10, 20, 30, 40, 50}
	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withParallelizedStreamingIterableInCursor[int, int](
		context.Background(),
		ci,
		"iter",
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

		Sections: []string{},
	}, limits)
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
		"iter",
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

		Sections: []string{},
	}, limits)
	require.NoError(t, err)

	encountered := []int{}
	lock := sync.Mutex{}

	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withDatastoreCursorInCursor[int, int](
		context.Background(),
		ci,
		"db",
		parentStream,
		5,
		func(queryCursor options.Cursor) ([]itemAndPostCursor[int], error) {
			return []itemAndPostCursor[int]{
				{1, tuple.MustParse("document:foo#viewer@user:tom")},
				{2, tuple.MustParse("document:foo#viewer@user:sarah")},
				{3, tuple.MustParse("document:foo#viewer@user:fred")},
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

		Sections: []string{"db", "", "somesection", "42"},
	}, limits)
	require.NoError(t, err)

	encountered := []int{}
	lock := sync.Mutex{}

	parentStream := dispatch.NewCollectingDispatchStream[int](context.Background())
	err = withDatastoreCursorInCursor[int, int](
		context.Background(),
		ci,
		"db",
		parentStream,
		5,
		func(queryCursor options.Cursor) ([]itemAndPostCursor[int], error) {
			require.Equal(t, "", tuple.MustString(queryCursor))

			return []itemAndPostCursor[int]{
				{2, tuple.MustParse("document:foo#viewer@user:sarah")},
				{3, tuple.MustParse("document:foo#viewer@user:fred")},
			}, nil
		},
		func(ctx context.Context, cc cursorInformation, item int, stream dispatch.Stream[int]) error {
			lock.Lock()
			encountered = append(encountered, item)
			lock.Unlock()

			if ok, _ := cc.hasHeadSection("somesection"); ok {
				value, _ := cc.integerSectionValue("somesection")
				item = item + value
			}

			return stream.Publish(item * 10)
		})

	require.NoError(t, err)

	expected := []int{440, 30}
	require.Equal(t, len(expected), len(encountered))
	require.Equal(t, expected, parentStream.Results())
}
