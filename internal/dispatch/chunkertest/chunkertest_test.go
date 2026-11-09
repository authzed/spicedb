package chunkertest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestByFirstByte(t *testing.T) {
	c := &chunkertest.ByFirstByte{}
	chunks := c.Chunk(t.Context(), "group", []string{"b1", "a1", "b2", "a2", "b3"}, 2)
	require.Equal(t, []dispatch.Chunk{
		{ResourceIDs: []string{"b1", "b2"}, Hint: chunkertest.HintFor("group", "b1")},
		{ResourceIDs: []string{"b3"}, Hint: chunkertest.HintFor("group", "b3")},
		{ResourceIDs: []string{"a1", "a2"}, Hint: chunkertest.HintFor("group", "a1")},
	}, chunks)
	require.Equal(t, []chunkertest.Call{
		{Namespace: "group", IDs: []string{"b1", "a1", "b2", "a2", "b3"}, MaxChunkSize: 2},
	}, c.Calls())
}

func TestRecorder(t *testing.T) {
	r := &chunkertest.Recorder{FailOn: "b1"}
	hinted := dispatch.WithRoutingHint(t.Context(), chunkertest.HintFor("group", "a1"))
	_, err := r.DispatchCheck(hinted, &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("group", "member").ToCoreRR(),
		ResourceIds:      []string{"a2", "a1"},
	})
	require.NoError(t, err)
	_, err = r.DispatchCheck(t.Context(), &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("group", "member").ToCoreRR(),
		ResourceIds:      []string{"b1"},
	})
	require.ErrorContains(t, err, "injected failure")

	require.Equal(t, []chunkertest.Dispatch{
		{Relation: "group#member", IDs: []string{"a1", "a2"}, Hint: chunkertest.HintFor("group", "a1"), HasHint: true},
		{Relation: "group#member", IDs: []string{"b1"}},
	}, r.Dispatches())
}

func TestRecorderLookupSubjects(t *testing.T) {
	r := &chunkertest.Recorder{FailOn: "b1"}
	hinted := dispatch.WithRoutingHint(t.Context(), chunkertest.HintFor("group", "a1"))
	err := r.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: tuple.RR("group", "member").ToCoreRR(),
		ResourceIds:      []string{"a1"},
	}, dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](hinted))
	require.NoError(t, err)

	err = r.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: tuple.RR("group", "member").ToCoreRR(),
		ResourceIds:      []string{"b1"},
	}, dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context()))
	require.ErrorContains(t, err, "injected failure")

	require.Equal(t, []chunkertest.Dispatch{
		{Relation: "group#member", IDs: []string{"a1"}, Hint: chunkertest.HintFor("group", "a1"), HasHint: true},
		{Relation: "group#member", IDs: []string{"b1"}},
	}, r.Dispatches())
}

func TestRecorderUnsupported(t *testing.T) {
	r := &chunkertest.Recorder{}
	_, err := r.DispatchExpand(t.Context(), &v1.DispatchExpandRequest{})
	require.ErrorContains(t, err, "supports only")
}

func TestRelationships(t *testing.T) {
	rels, err := chunkertest.Relationships()
	require.NoError(t, err)
	require.Len(t, rels, 6)
}
