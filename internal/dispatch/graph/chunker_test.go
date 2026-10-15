package graph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// newChunkingDispatcher returns a graph dispatcher over chunkertest.Schema.
// The returned context carries chunkertest.ByFirstByte. The dispatcher redispatches to the returned recorder.
func newChunkingDispatcher(t *testing.T) (context.Context, dispatch.Dispatcher, *chunkertest.Recorder, datastore.Revision) {
	t.Helper()
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)
	rels, err := chunkertest.Relationships()
	require.NoError(t, err)
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, chunkertest.Schema, rels)

	recorder := &chunkertest.Recorder{}
	params := MustNewDefaultDispatcherParametersForTesting()
	d, err := NewDispatcher(recorder, params)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	ctx := log.Logger.WithContext(datalayer.ContextWithHandle(dispatch.ContextWithChunker(t.Context(), &chunkertest.ByFirstByte{})))
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))
	return ctx, d, recorder, revision
}

func chunkingMetadata(revision datastore.Revision) *v1.ResolverMeta {
	return &v1.ResolverMeta{
		AtRevision:     revision.String(),
		DepthRemaining: 50,
		SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
	}
}

func chunkingCheckRequest(revision datastore.Revision, relation string) *v1.DispatchCheckRequest {
	return &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", relation).ToCoreRR(),
		ResourceIds:      []string{"doc1"},
		ResultsSetting:   v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		Subject:          tuple.ONR("user", "tom", tuple.Ellipsis).ToCoreONR(),
		Metadata:         chunkingMetadata(revision),
	}
}

// groupChunks returns the group#member dispatches that ByFirstByte forms from chunkertest.Relationships.
func groupChunks() []chunkertest.Dispatch {
	return []chunkertest.Dispatch{
		{Relation: "group#member", IDs: []string{"a1", "a2"}, Hint: chunkertest.HintFor("group", "a1"), HasHint: true},
		{Relation: "group#member", IDs: []string{"b1"}, Hint: chunkertest.HintFor("group", "b1"), HasHint: true},
	}
}

func TestCheckDispatchesCarryChunkHints(t *testing.T) {
	// viewer is a direct userset, parent_member is an arrow, and all_parent_member is an intersection arrow.
	for _, relation := range []string{"viewer", "parent_member", "all_parent_member"} {
		t.Run(relation, func(t *testing.T) {
			ctx, d, recorder, revision := newChunkingDispatcher(t)
			_, err := d.DispatchCheck(ctx, chunkingCheckRequest(revision, relation))
			require.NoError(t, err)
			require.ElementsMatch(t, groupChunks(), recorder.Dispatches())
		})
	}
}

func TestComputedUsersetDispatchDropsOuterHint(t *testing.T) {
	ctx, d, recorder, revision := newChunkingDispatcher(t)
	ctx = dispatch.WithRoutingHint(ctx, chunkertest.HintFor("document", "doc1"))
	_, err := d.DispatchCheck(ctx, chunkingCheckRequest(revision, "view"))
	require.NoError(t, err)

	want := append(groupChunks(), chunkertest.Dispatch{Relation: "document#viewer", IDs: []string{"doc1"}})
	require.ElementsMatch(t, want, recorder.Dispatches(),
		"the computed-userset forward has no hint, and the arrow chunks have their own hints")
}

func chunkingLookupSubjectsRequest(revision datastore.Revision, relation string) *v1.DispatchLookupSubjectsRequest {
	return &v1.DispatchLookupSubjectsRequest{
		ResourceRelation: tuple.RR("document", relation).ToCoreRR(),
		ResourceIds:      []string{"doc1"},
		SubjectRelation:  tuple.RR("user", tuple.Ellipsis).ToCoreRR(),
		Metadata:         chunkingMetadata(revision),
	}
}

func TestLookupSubjectsDispatchesCarryChunkHints(t *testing.T) {
	// viewer is a direct userset, and parent_member is an arrow.
	for _, relation := range []string{"viewer", "parent_member"} {
		t.Run(relation, func(t *testing.T) {
			ctx, d, recorder, revision := newChunkingDispatcher(t)
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
			require.NoError(t, d.DispatchLookupSubjects(chunkingLookupSubjectsRequest(revision, relation), stream))
			require.ElementsMatch(t, groupChunks(), recorder.Dispatches())
		})
	}
}

func TestLookupSubjectsComputedDispatchDropsOuterHint(t *testing.T) {
	ctx, d, recorder, revision := newChunkingDispatcher(t)
	ctx = dispatch.WithRoutingHint(ctx, chunkertest.HintFor("document", "doc1"))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	require.NoError(t, d.DispatchLookupSubjects(chunkingLookupSubjectsRequest(revision, "view"), stream))

	want := append(groupChunks(), chunkertest.Dispatch{Relation: "document#viewer", IDs: []string{"doc1"}})
	require.ElementsMatch(t, want, recorder.Dispatches(),
		"the computed-userset forward has no hint, and the arrow chunks have their own hints")
}

// checksToRecorder sends Check dispatches to rec and every other dispatch to the embedded dispatcher.
type checksToRecorder struct {
	dispatch.Dispatcher
	rec *chunkertest.Recorder
}

func (c *checksToRecorder) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return c.rec.DispatchCheck(ctx, req)
}

// newLookupResourcesChunkingDispatcher returns a graph dispatcher over chunkertest.Schema.
// The returned context carries chunkertest.ByFirstByte. The dispatcher sends its Check dispatches to the returned recorder.
// user:tom is a member of group:g1, which is the parent of document:x1, document:x2 and document:y1.
func newLookupResourcesChunkingDispatcher(t *testing.T) (context.Context, dispatch.Dispatcher, *chunkertest.Recorder, datastore.Revision) {
	t.Helper()
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)
	rels := []tuple.Relationship{
		tuple.MustParse("group:g1#member@user:tom"),
		tuple.MustParse("document:x1#parent@group:g1"),
		tuple.MustParse("document:x2#parent@group:g1"),
		tuple.MustParse("document:y1#parent@group:g1"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, chunkertest.Schema, rels)

	recorder := &chunkertest.Recorder{}
	redispatcher := &checksToRecorder{rec: recorder}
	params := MustNewDefaultDispatcherParametersForTesting()
	d, err := NewDispatcher(redispatcher, params)
	require.NoError(t, err)
	redispatcher.Dispatcher = d
	t.Cleanup(func() { _ = d.Close() })

	ctx := log.Logger.WithContext(datalayer.ContextWithHandle(dispatch.ContextWithChunker(t.Context(), &chunkertest.ByFirstByte{})))
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))
	return ctx, d, recorder, revision
}

// documentChunks returns the document#all_parent_member checks that ByFirstByte forms for the documents of user:tom.
func documentChunks() []chunkertest.Dispatch {
	return []chunkertest.Dispatch{
		{Relation: "document#all_parent_member", IDs: []string{"x1", "x2"}, Hint: chunkertest.HintFor("document", "x1"), HasHint: true},
		{Relation: "document#all_parent_member", IDs: []string{"y1"}, Hint: chunkertest.HintFor("document", "y1"), HasHint: true},
	}
}

func TestLookupResources2ChecksCarryChunkHints(t *testing.T) {
	ctx, d, recorder, revision := newLookupResourcesChunkingDispatcher(t)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
	require.NoError(t, d.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: tuple.RR("document", "all_parent_member").ToCoreRR(),
		SubjectRelation:  tuple.RR("user", tuple.Ellipsis).ToCoreRR(),
		SubjectIds:       []string{"tom"},
		TerminalSubject:  tuple.ONR("user", "tom", tuple.Ellipsis).ToCoreONR(),
		Metadata:         chunkingMetadata(revision),
	}, stream))
	require.ElementsMatch(t, documentChunks(), recorder.Dispatches())
}

func TestLookupResources3ChecksCarryChunkHints(t *testing.T) {
	ctx, d, recorder, revision := newLookupResourcesChunkingDispatcher(t)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
	require.NoError(t, d.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
		ResourceRelation: tuple.RR("document", "all_parent_member").ToCoreRR(),
		SubjectRelation:  tuple.RR("user", tuple.Ellipsis).ToCoreRR(),
		SubjectIds:       []string{"tom"},
		TerminalSubject:  tuple.ONR("user", "tom", tuple.Ellipsis).ToCoreONR(),
		Metadata:         chunkingMetadata(revision),
	}, stream))
	require.ElementsMatch(t, documentChunks(), recorder.Dispatches())
}
