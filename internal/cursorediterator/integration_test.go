package cursorediterator

import (
	"context"
	"iter"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursoredIterators(t *testing.T) {
	tcs := []struct {
		name          string
		cursor        Cursor
		limit         int
		expectedItems []string
	}{
		{
			name:          "test with empty Cursor",
			cursor:        nil,
			limit:         5,
			expectedItems: []string{"a", "b", "c", "d", "e"},
		},
		{
			name:          "test with non-empty Cursor",
			cursor:        Cursor{"3"},
			limit:         5,
			expectedItems: []string{"d", "e", "f", "g", "h"},
		},
		{
			name:          "limit beyond available items",
			cursor:        nil,
			limit:         15,
			expectedItems: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "aa", "bb", "cc", "dd", "ee"},
		},
		{
			name:          "cursor beyond the first iterator",
			cursor:        Cursor{"10"},
			limit:         5,
			expectedItems: []string{"aa", "bb", "cc", "dd", "ee"},
		},
		{
			name:          "cursor beyond the first iterator with larger limit",
			cursor:        Cursor{"10"},
			limit:         10,
			expectedItems: []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"},
		},
		{
			name:          "into parallel iterators, with limit",
			cursor:        Cursor{"5", "-1"},
			limit:         8,
			expectedItems: []string{"ff", "gg", "hh", "ii", "jj", "subit1-a", "subit1-b", "subit1-c"},
		},
		{
			name:          "into parallel iterators",
			cursor:        Cursor{"5", "-1"},
			limit:         10,
			expectedItems: []string{"ff", "gg", "hh", "ii", "jj", "subit1-a", "subit1-b", "subit1-c", "subit1-d", "subit1-e"},
		},
		{
			name:          "second into parallel iterators",
			cursor:        Cursor{"1", "-1", "-1"},
			limit:         7,
			expectedItems: []string{"subit2-a", "subit2-b", "subit2-c", "subit2-d", "subit3-a", "subit3-b", "subit3-c"},
		},
		{
			name:          "third into parallel iterators",
			cursor:        Cursor{"2", "-1", "-1"},
			limit:         5,
			expectedItems: []string{"subit3-a", "subit3-b", "subit3-c", "subit3-d", "subit3-e"},
		},
		{
			name:          "fourth into parallel iterators",
			cursor:        Cursor{"3", "-1", "-1"},
			limit:         5,
			expectedItems: []string{"mapped-premapped-a", "mapped-premapped-b", "mapped-premapped-c", "mapped-premapped-d", "mapped-premapped-e"},
		},
		{
			name:          "fourth into parallel iterators with smaller limit",
			cursor:        Cursor{"3", "-1", "-1"},
			limit:         4,
			expectedItems: []string{"mapped-premapped-a", "mapped-premapped-b", "mapped-premapped-c", "mapped-premapped-d"},
		},
		{
			name:          "fourth into parallel iterators with offset into chunks",
			cursor:        Cursor{"2", "3", "-1", "-1"},
			limit:         5,
			expectedItems: []string{"mapped-premapped-f", "mapped-premapped-g", "mapped-premapped-h"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			results := iteratorForTest(ctx, tc.cursor)
			itemsCollected := make([]string, 0, tc.limit)

			for item, err := range results {
				require.NoError(t, err)
				itemsCollected = append(itemsCollected, item.Item)
				if len(itemsCollected) >= tc.limit {
					break
				}
			}

			require.Equal(t, tc.expectedItems, itemsCollected, "Expected items do not match collected items")
		})
	}
}

func TestCursoredIteratorsFromEveryCursor(t *testing.T) {
	fullExpectedData := []any{
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
		"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj",
		"subit1-a", "subit1-b", "subit1-c", "subit1-d", "subit1-e",
		"subit2-a", "subit2-b", "subit2-c", "subit2-d",
		"subit3-a", "subit3-b", "subit3-c", "subit3-d", "subit3-e",
		"mapped-premapped-a", "mapped-premapped-b", "mapped-premapped-c",
		"mapped-premapped-d", "mapped-premapped-e", "mapped-premapped-f",
		"mapped-premapped-g", "mapped-premapped-h",
	}

	currentCursor := Cursor{}
	for index := range len(fullExpectedData) {
		results := iteratorForTest(t.Context(), currentCursor)
		for r := range results {
			require.Equal(t, fullExpectedData[index], r.Item, "Expected item does not match collected item at index %d", index)
			currentCursor = r.Cursor
			break
		}
	}

	results := iteratorForTest(t.Context(), currentCursor)
	for r := range results {
		require.Failf(t, "Expected no more items, but got: %v", r.Item)
	}
}

func basicIteratorForTestData(testData ...string) func(ctx context.Context, startIndex int) iter.Seq2[string, error] {
	return func(ctx context.Context, startIndex int) iter.Seq2[string, error] {
		return func(yield func(string, error) bool) {
			for _, s := range testData[startIndex:] {
				if !yield(s, nil) {
					return
				}
			}
		}
	}
}

func manuallyCursoredIteratorForTestData(testData ...string) func(ctx context.Context, Cursor Cursor) iter.Seq2[ItemAndCursor[string], error] {
	return func(ctx context.Context, currentCursor Cursor) iter.Seq2[ItemAndCursor[string], error] {
		startIndex, _, err := CursorIntHeadValue(currentCursor)
		if err != nil {
			return YieldsError[ItemAndCursor[string]](err)
		}

		return func(yield func(ItemAndCursor[string], error) bool) {
			for _, s := range testData[startIndex:] {
				if !yield(ItemAndCursor[string]{
					Item:   s,
					Cursor: Cursor{strconv.Itoa(startIndex + 1)},
				}, nil) {
					return
				}
			}
		}
	}
}

func iteratorForTest(ctx context.Context, currentCursor Cursor) iter.Seq2[ItemAndCursor[string], error] {
	return CursoredWithIntegerHeader(
		ctx,
		currentCursor,
		basicIteratorForTestData("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"),
		iteratorPart2,
	)
}

func iteratorPart2(ctx context.Context, currentCursor Cursor) iter.Seq2[ItemAndCursor[string], error] {
	return CursoredWithIntegerHeader(
		ctx,
		currentCursor,
		basicIteratorForTestData("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"),
		iteratorPart3,
	)
}

func iteratorPart3(ctx context.Context, currentCursor Cursor) iter.Seq2[ItemAndCursor[string], error] {
	return CursoredParallelIterators(
		ctx,
		currentCursor,
		2,
		manuallyCursoredIteratorForTestData("subit1-a", "subit1-b", "subit1-c", "subit1-d", "subit1-e"),
		manuallyCursoredIteratorForTestData("subit2-a", "subit2-b", "subit2-c", "subit2-d"),
		manuallyCursoredIteratorForTestData("subit3-a", "subit3-b", "subit3-c", "subit3-d", "subit3-e"),
		producerMapperTestIterator,
	)
}

func producerMapperTestIterator(ctx context.Context, currentCursor Cursor) iter.Seq2[ItemAndCursor[string], error] {
	return CursoredProducerMapperIterator(
		ctx,
		currentCursor,
		1, // concurrency
		intFromString,
		stringToInt,
		func(ctx context.Context, startIndex int, remainingCursor Cursor) iter.Seq2[ChunkOrHold[[]string, int], error] {
			chunks := []Chunk[[]string, int]{
				{CurrentChunk: []string{"premapped-a", "premapped-b", "premapped-c"}, CurrentChunkCursor: 0},
				{CurrentChunk: []string{"premapped-d", "premapped-e"}, CurrentChunkCursor: 1},
				{CurrentChunk: []string{"premapped-f", "premapped-g", "premapped-h"}, CurrentChunkCursor: 2},
			}

			return func(yield func(ChunkOrHold[[]string, int], error) bool) {
				for _, chunk := range chunks[startIndex:] {
					if !yield(chunk, nil) {
						return
					}
				}
			}
		},
		func(ctx context.Context, remainingCursor Cursor, chunk []string) iter.Seq2[ItemAndCursor[string], error] {
			startIndex, _, err := CursorIntHeadValue(remainingCursor)
			if err != nil {
				return YieldsError[ItemAndCursor[string]](err)
			}

			return func(yield func(ItemAndCursor[string], error) bool) {
				currentIndex := 0
				for _, item := range chunk {
					if currentIndex < startIndex {
						currentIndex++
						continue
					}

					nextIndexStr, _ := stringToInt(currentIndex + 1)
					if !yield(ItemAndCursor[string]{Item: "mapped-" + item, Cursor: Cursor{nextIndexStr}}, nil) {
						return
					}
					currentIndex++
				}
			}
		},
	)
}

func intFromString(str string) (int, error) {
	if str == "" {
		return 0, nil
	}
	return strconv.Atoi(str)
}

func stringToInt(i int) (string, error) {
	return strconv.Itoa(i), nil
}
