package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// cyclicGroupSchema defines a group schema where two groups are mutual members —
// the minimal cyclic data pattern that exercises cycle detection.
const cyclicGroupSchema = `
definition user {}

definition group {
	relation member: user | group#member
	permission can_access = member
}`

func TestLookupSubjectsCycleDetection(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	rels := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member"),
		tuple.MustParse("group:b#member@group:a#member"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, cyclicGroupSchema, rels, require.New(t))

	dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	t.Cleanup(func() { dispatcher.Close() })

	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	req := &v1.DispatchLookupSubjectsRequest{
		ResourceRelation: RR("group", "can_access").ToCoreRR(),
		ResourceIds:      []string{"a"},
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		EnableDebugTrace: true,
	}

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	err = dispatcher.DispatchLookupSubjects(req, stream)
	// With cyclic data the bloom filter terminates traversal; a depth-exceeded
	// error is acceptable. The critical assertion is that the call terminates.
	if err != nil {
		require.Contains(t, err.Error(), "max depth exceeded",
			"unexpected error for cyclic data with debug trace enabled: %v", err)
		return
	}
	// No error means the bloom filter caught the cycle early — still valid.
	require.NotNil(t, stream)
}

func TestLookupResources3CycleDetection(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	rels := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member"),
		tuple.MustParse("group:b#member@group:a#member"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, cyclicGroupSchema, rels, require.New(t))

	dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	t.Cleanup(func() { dispatcher.Close() })

	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	req := &v1.DispatchLookupResources3Request{
		ResourceRelation: RR("group", "can_access").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"someone"},
		TerminalSubject:  tuple.CoreONR("user", "someone", graph.Ellipsis),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		EnableDebugTrace: true,
	}

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
	err = dispatcher.DispatchLookupResources3(req, stream)
	if err != nil {
		require.Contains(t, err.Error(), "max depth exceeded",
			"unexpected error for cyclic data with debug trace enabled: %v", err)
		return
	}
	// Success: stream is valid even with zero results (no user found the group).
	require.NotNil(t, stream)
}
