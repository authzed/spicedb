package computed_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	"github.com/authzed/spicedb/internal/graph/computed"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func chunkerCheckParameters() computed.CheckParameters {
	return computed.CheckParameters{
		ResourceType: tuple.RR("document", "view"),
		Subject:      tuple.ONR("user", "tom", tuple.Ellipsis),
		AtRevision:   datastore.NoRevision,
		MaximumDepth: 50,
		DebugOption:  computed.NoDebugging,
		SchemaHash:   datalayer.NoSchemaHashForTesting,
	}
}

func documentDispatch(firstID string, ids ...string) chunkertest.Dispatch {
	return chunkertest.Dispatch{
		Relation: "document#view",
		IDs:      ids,
		Hint:     chunkertest.HintFor("document", firstID),
		HasHint:  true,
	}
}

func TestComputeBulkCheckDispatchesOwnerChunks(t *testing.T) {
	recorder := &chunkertest.Recorder{}
	chunker := &chunkertest.ByFirstByte{}
	results, _, _, err := computed.ComputeBulkCheck(dispatch.ContextWithChunker(t.Context(), chunker), recorder, caveattypes.Default.TypeSet,
		chunkerCheckParameters(), []string{"a1", "b1", "a2"}, 100)
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.Equal(t, []chunkertest.Dispatch{
		documentDispatch("a1", "a1", "a2"),
		documentDispatch("b1", "b1"),
	}, recorder.Dispatches())
	require.Equal(t, []chunkertest.Call{
		{Namespace: "document", IDs: []string{"a1", "b1", "a2"}, MaxChunkSize: 100},
	}, chunker.Calls())
}

func TestComputeBulkCheckStopsAtFirstFailedChunk(t *testing.T) {
	recorder := &chunkertest.Recorder{FailOn: "b1"}
	results, _, _, err := computed.ComputeBulkCheck(dispatch.ContextWithChunker(t.Context(), &chunkertest.ByFirstByte{}), recorder, caveattypes.Default.TypeSet,
		chunkerCheckParameters(), []string{"a1", "b1", "c1"}, 100)
	require.ErrorContains(t, err, "injected failure")
	require.Equal(t, []chunkertest.Dispatch{
		documentDispatch("a1", "a1"),
		documentDispatch("b1", "b1"),
	}, recorder.Dispatches(), "the chunk after the failed chunk is not dispatched")
	require.Contains(t, results, "a1")
	require.NotContains(t, results, "c1")
}

func TestComputeBulkCheckChunkDoesNotChunkAgain(t *testing.T) {
	recorder := &chunkertest.Recorder{}
	chunker := &chunkertest.ByFirstByte{}
	chunk := dispatch.Chunk{ResourceIDs: []string{"a1", "b1"}, Hint: chunkertest.HintFor("document", "a1")}
	results, _, _, err := computed.ComputeBulkCheckChunk(dispatch.ContextWithChunker(t.Context(), chunker), recorder, caveattypes.Default.TypeSet,
		chunkerCheckParameters(), chunk)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, []chunkertest.Dispatch{documentDispatch("a1", "a1", "b1")}, recorder.Dispatches())
	require.Empty(t, chunker.Calls(), "a pre-formed chunk is dispatched as given, so no key is touched again")
}

func TestComputeBulkCheckWithoutChunkerSplitsBySize(t *testing.T) {
	recorder := &chunkertest.Recorder{}
	_, _, _, err := computed.ComputeBulkCheck(t.Context(), recorder, caveattypes.Default.TypeSet,
		chunkerCheckParameters(), []string{"a1", "b1", "a2"}, 2)
	require.NoError(t, err)
	require.Equal(t, []chunkertest.Dispatch{
		{Relation: "document#view", IDs: []string{"a1", "b1"}},
		{Relation: "document#view", IDs: []string{"a2"}},
	}, recorder.Dispatches())
}
