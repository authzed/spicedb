package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
)

func TestBranchContextKeepsChunker(t *testing.T) {
	chunker := &chunkertest.ByFirstByte{}
	ctx, cancel := branchContext(dispatch.ContextWithChunker(t.Context(), chunker))
	defer cancel(nil)
	require.Same(t, chunker, dispatch.ChunkerFromContext(ctx))
}

func TestBranchContextWithoutChunker(t *testing.T) {
	ctx, cancel := branchContext(t.Context())
	defer cancel(nil)
	require.Equal(t, dispatch.DefaultChunker, dispatch.ChunkerFromContext(ctx))
}
