package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestClusterDispatcherUsesContextChunker checks that the graph of a receiving node chunks with the chunker of the context.
func TestClusterDispatcherUsesContextChunker(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)
	rels, err := chunkertest.Relationships()
	require.NoError(t, err)
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, chunkertest.Schema, rels)

	recorder := &chunkertest.Recorder{}
	d, err := NewClusterDispatcher(recorder, DispatchChunkSize(100))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	ctx := datalayer.ContextWithHandle(dispatch.ContextWithChunker(t.Context(), &chunkertest.ByFirstByte{}))
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))
	_, err = d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "viewer").ToCoreRR(),
		ResourceIds:      []string{"doc1"},
		ResultsSetting:   v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		Subject:          tuple.ONR("user", "tom", tuple.Ellipsis).ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []chunkertest.Dispatch{
		{Relation: "group#member", IDs: []string{"a1", "a2"}, Hint: chunkertest.HintFor("group", "a1"), HasHint: true},
		{Relation: "group#member", IDs: []string{"b1"}, Hint: chunkertest.HintFor("group", "b1"), HasHint: true},
	}, recorder.Dispatches())
}
