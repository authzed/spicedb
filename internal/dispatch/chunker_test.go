package dispatch

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/slicez"
)

func TestDefaultChunkerMatchesForEachChunk(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	for _, size := range []uint16{0, 1, 2, 5, 100} {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			var want [][]string
			slicez.ForEachChunk(ids, size, func(part []string) {
				want = append(want, part)
			})

			chunks := DefaultChunker.Chunk(t.Context(), "document", ids, size)
			got := make([][]string, 0, len(chunks))
			for _, chunk := range chunks {
				require.True(t, chunk.Hint.IsZero(), "the default chunker gives no hint")
				got = append(got, chunk.ResourceIDs)
			}
			require.Equal(t, want, got)
		})
	}
	require.Empty(t, DefaultChunker.Chunk(t.Context(), "document", nil, 2))
}

type otherChunker struct {
	sizeChunker
}

func TestChunkerFromContext(t *testing.T) {
	t.Run("no chunker selects the default", func(t *testing.T) {
		require.Equal(t, DefaultChunker, ChunkerFromContext(t.Context()))
	})

	t.Run("stored chunker is returned", func(t *testing.T) {
		ctx := ContextWithChunker(t.Context(), otherChunker{})
		require.Equal(t, otherChunker{}, ChunkerFromContext(ctx))
	})

	t.Run("nil chunker selects the default", func(t *testing.T) {
		ctx := ContextWithChunker(t.Context(), nil)
		require.Equal(t, DefaultChunker, ChunkerFromContext(ctx))
	})

	t.Run("nil chunker replaces an outer chunker", func(t *testing.T) {
		outer := ContextWithChunker(t.Context(), otherChunker{})
		require.Equal(t, DefaultChunker, ChunkerFromContext(ContextWithChunker(outer, nil)))
	})

	t.Run("pointer chunker is returned", func(t *testing.T) {
		chunker := &otherChunker{}
		ctx := ContextWithChunker(t.Context(), chunker)
		require.Same(t, chunker, ChunkerFromContext(ctx))
	})
}

func TestRoutingHintRoundTrip(t *testing.T) {
	hint := RoutingHint{Key: []byte("document:a"), Owner: "n1"}
	got, ok := RoutingHintFrom(WithRoutingHint(t.Context(), hint))
	require.True(t, ok)
	require.Equal(t, hint, got)
}

func TestRoutingHintFromContextWithoutHint(t *testing.T) {
	_, ok := RoutingHintFrom(t.Context())
	require.False(t, ok)
}

func TestZeroRoutingHintClearsOuterHint(t *testing.T) {
	outer := WithRoutingHint(t.Context(), RoutingHint{Key: []byte("document:a"), Owner: "n1"})
	_, ok := RoutingHintFrom(WithRoutingHint(outer, RoutingHint{}))
	require.False(t, ok, "a dispatch without a hint never uses the hint of an outer dispatch")
}

func TestWithRoutingHintIfNeeded(t *testing.T) {
	hint := RoutingHint{Key: []byte("document:a")}

	t.Run("no hint and zero hint keeps ctx", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.Equal(t, ctx, WithRoutingHintIfNeeded(ctx, RoutingHint{}))
	})

	t.Run("zero hint clears inherited hint", func(t *testing.T) {
		ctx := WithRoutingHint(context.Background(), hint)
		_, ok := RoutingHintFrom(ctx)
		require.True(t, ok)

		_, ok = RoutingHintFrom(WithRoutingHintIfNeeded(ctx, RoutingHint{}))
		require.False(t, ok)
	})

	t.Run("nonzero hint is set", func(t *testing.T) {
		got, ok := RoutingHintFrom(WithRoutingHintIfNeeded(context.Background(), hint))
		require.True(t, ok)
		require.Equal(t, hint, got)
	})
}
