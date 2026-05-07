package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
)

// testDispatcher is a minimal dispatcher that records DispatchQueryPlan calls
// and writes configurable responses to the stream.
type testDispatcher struct {
	planCalls     []*v1.DispatchQueryPlanRequest
	planResponses []*v1.DispatchQueryPlanResponse
	planErr       error
}

func (d *testDispatcher) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{}, nil
}

func (d *testDispatcher) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (d *testDispatcher) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ LookupResources2Stream) error {
	return nil
}

func (d *testDispatcher) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ LookupResources3Stream) error {
	return nil
}

func (d *testDispatcher) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ LookupSubjectsStream) error {
	return nil
}

func (d *testDispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream PlanStream) error {
	d.planCalls = append(d.planCalls, req)
	if d.planErr != nil {
		return d.planErr
	}
	for _, resp := range d.planResponses {
		if err := stream.Publish(resp); err != nil {
			return err
		}
	}
	return nil
}
func (d *testDispatcher) Close() error           { return nil }
func (d *testDispatcher) ReadyState() ReadyState { return ReadyState{IsReady: true} }

var _ Dispatcher = &testDispatcher{}

func newTestContext() *query.Context {
	return query.NewLocalContext(context.Background())
}

func TestDispatchExecutor_Check_AliasDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{{
				ResourceType:    "document",
				ResourceId:      "doc1",
				Relation:        "viewer",
				SubjectType:     "user",
				SubjectId:       "alice",
				SubjectRelation: "...",
			}},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// AliasIterator wrapping a FixedIterator — should trigger dispatch
	inner := query.NewFixedIterator()
	alias := query.NewAliasIterator("", "viewer", inner)

	path, err := sender.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "document", path.Resource.ObjectType)
	require.Equal(t, "doc1", path.Resource.ObjectID)
	require.Equal(t, "alice", path.Subject.ObjectID)

	// Verify dispatch was called
	require.Len(t, receiver.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_CHECK, receiver.planCalls[0].Operation)
	require.Equal(t, "rev1", receiver.planCalls[0].PlanContext.Revision)
}

func TestDispatchExecutor_Check_NonAliasLocal(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// FixedIterator (not an alias) — should delegate locally, no dispatch
	fixed := query.NewFixedIterator(query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation: "viewer",
		Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
	})

	path, err := sender.Check(ctx, fixed, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "alice", path.Subject.ObjectID)

	// Verify NO dispatch was called
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_Check_AliasNoResult(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{}, // empty = no permission
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())

	path, err := sender.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.Nil(t, path)
	require.Len(t, receiver.planCalls, 1)
}

func TestDispatchExecutor_IterSubjects_AliasDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "bob", SubjectRelation: "..."},
			},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())

	pathSeq, err := sender.IterSubjects(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.Equal(t, "alice", paths[0].Subject.ObjectID)
	require.Equal(t, "bob", paths[1].Subject.ObjectID)

	require.Len(t, receiver.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS, receiver.planCalls[0].Operation)
}

func TestDispatchExecutor_IterSubjects_NonAliasLocal(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	fixed := query.NewFixedIterator(
		query.Path{
			Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
			Relation: "viewer",
			Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
		query.Path{
			Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
			Relation: "viewer",
			Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "bob", Relation: "..."},
		},
	)

	pathSeq, err := sender.IterSubjects(ctx, fixed, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_IterResources_AliasDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
				{ResourceType: "document", ResourceId: "doc2", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
			},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())

	pathSeq, err := sender.IterResources(ctx, alias, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.Equal(t, "doc1", paths[0].Resource.ObjectID)
	require.Equal(t, "doc2", paths[1].Resource.ObjectID)

	require.Len(t, receiver.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES, receiver.planCalls[0].Operation)
}

func TestDispatchExecutor_IterResources_NonAliasLocal(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	fixed := query.NewFixedIterator(
		query.Path{
			Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
			Relation: "viewer",
			Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
	)

	pathSeq, err := sender.IterResources(ctx, fixed, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_Check_AliasWithRecursiveSentinelDoesNotDispatch(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// AliasIterator wrapping a RecursiveSentinel — should NOT dispatch
	sentinel := query.NewRecursiveSentinelIterator("document", "viewer", false)
	alias := query.NewAliasIterator("", "viewer", sentinel)

	path, err := sender.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	// RecursiveSentinel.CheckImpl returns nil
	require.Nil(t, path)

	// Verify NO dispatch was called
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_IterSubjects_AliasWithRecursiveSentinelDoesNotDispatch(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	sentinel := query.NewRecursiveSentinelIterator("document", "viewer", false)
	alias := query.NewAliasIterator("", "viewer", sentinel)

	pathSeq, err := sender.IterSubjects(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Empty(t, paths)
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_IterResources_AliasWithRecursiveSentinelDoesNotDispatch(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	sentinel := query.NewRecursiveSentinelIterator("document", "viewer", false)
	alias := query.NewAliasIterator("", "viewer", sentinel)

	pathSeq, err := sender.IterResources(ctx, alias, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Empty(t, paths)
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_Check_AliasWithSentinelInsideRecursiveDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{{
				ResourceType:    "document",
				ResourceId:      "doc1",
				Relation:        "viewer",
				SubjectType:     "user",
				SubjectId:       "alice",
				SubjectRelation: "...",
			}},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// AliasIterator wrapping a RecursiveIterator that contains a RecursiveSentinel.
	// The sentinel is managed by the RecursiveIterator's context, so dispatch is allowed.
	sentinel := query.NewRecursiveSentinelIterator("document", "viewer", false)
	recursive := query.NewRecursiveIterator(sentinel, "document", "viewer")
	alias := query.NewAliasIterator("", "viewer", recursive)

	path, err := sender.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "alice", path.Subject.ObjectID)

	// Verify dispatch WAS called — the RecursiveIterator boundary makes it safe
	require.Len(t, receiver.planCalls, 1)
}

func TestDispatchExecutor_IterSubjects_AliasWithSentinelInsideRecursiveDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{{
				ResourceType:    "document",
				ResourceId:      "doc1",
				Relation:        "viewer",
				SubjectType:     "user",
				SubjectId:       "alice",
				SubjectRelation: "...",
			}},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	sentinel := query.NewRecursiveSentinelIterator("document", "viewer", false)
	recursive := query.NewRecursiveIterator(sentinel, "document", "viewer")
	alias := query.NewAliasIterator("", "viewer", recursive)

	pathSeq, err := sender.IterSubjects(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, "alice", paths[0].Subject.ObjectID)

	require.Len(t, receiver.planCalls, 1)
}

func TestDispatchExecutor_IterResources_AliasWithSentinelInsideRecursiveDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{{
				ResourceType:    "document",
				ResourceId:      "doc1",
				Relation:        "viewer",
				SubjectType:     "user",
				SubjectId:       "alice",
				SubjectRelation: "...",
			}},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	sentinel := query.NewRecursiveSentinelIterator("document", "viewer", false)
	recursive := query.NewRecursiveIterator(sentinel, "document", "viewer")
	alias := query.NewAliasIterator("", "viewer", recursive)

	pathSeq, err := sender.IterResources(ctx, alias, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, "doc1", paths[0].Resource.ObjectID)

	require.Len(t, receiver.planCalls, 1)
}

func TestDispatchExecutor_Check_DoubleRecursionUnmatchedSentinelDoesNotDispatch(t *testing.T) {
	receiver := &testDispatcher{}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// Double recursion: RecursiveIterator for "folder#viewer" contains a
	// RecursiveSentinelIterator for "document#reader" in its subtree.
	// The document#reader sentinel is unmatched (no RecursiveIterator for it),
	// so dispatch should be blocked.
	documentSentinel := query.NewRecursiveSentinelIterator("document", "reader", false)
	folderRecursive := query.NewRecursiveIterator(documentSentinel, "folder", "viewer")
	alias := query.NewAliasIterator("", "viewer", folderRecursive)

	path, err := sender.Check(ctx, alias, query.Object{ObjectType: "folder", ObjectID: "f1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.Nil(t, path)

	// Verify NO dispatch was called — unmatched sentinel blocks dispatch
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_Check_DoubleRecursionBothMatchedDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{{
				ResourceType:    "folder",
				ResourceId:      "f1",
				Relation:        "viewer",
				SubjectType:     "user",
				SubjectId:       "alice",
				SubjectRelation: "...",
			}},
		}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// Double recursion with both pairs matched:
	// RecursiveIterator(folder#viewer) contains RecursiveIterator(document#reader)
	// which contains RecursiveSentinelIterator(document#reader).
	// All sentinels have a matching iterator, so dispatch is allowed.
	documentSentinel := query.NewRecursiveSentinelIterator("document", "reader", false)
	documentRecursive := query.NewRecursiveIterator(documentSentinel, "document", "reader")
	folderRecursive := query.NewRecursiveIterator(documentRecursive, "folder", "viewer")
	alias := query.NewAliasIterator("", "viewer", folderRecursive)

	path, err := sender.Check(ctx, alias, query.Object{ObjectType: "folder", ObjectID: "f1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "alice", path.Subject.ObjectID)

	// Verify dispatch WAS called — all sentinels are matched
	require.Len(t, receiver.planCalls, 1)
}

func TestDispatchExecutor_StreamBatching(t *testing.T) {
	// Multiple streamed responses should be collected correctly
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{
			{Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
			}},
			{Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc2", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
				{ResourceType: "document", ResourceId: "doc3", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
			}},
		},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())

	pathSeq, err := sender.IterResources(ctx, alias, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 3)
	require.Equal(t, "doc1", paths[0].Resource.ObjectID)
	require.Equal(t, "doc2", paths[1].Resource.ObjectID)
	require.Equal(t, "doc3", paths[2].Resource.ObjectID)
}

func TestDispatchExecutor_PlanContextForwarded(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{Paths: []*v1.ResultPath{}}},
	}

	pc := &v1.PlanContext{
		Revision:               "rev42",
		MaxRecursionDepth:      5,
		OptionalDatastoreLimit: 100,
	}
	sender := NewDispatchExecutor(receiver, pc, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())
	_, _ = sender.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})

	require.Len(t, receiver.planCalls, 1)
	require.Equal(t, "rev42", receiver.planCalls[0].PlanContext.Revision)
	require.Equal(t, int32(5), receiver.planCalls[0].PlanContext.MaxRecursionDepth)
	require.Equal(t, uint64(100), receiver.planCalls[0].PlanContext.OptionalDatastoreLimit)
}

func TestDispatchExecutor_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{Paths: []*v1.ResultPath{}}},
	}

	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	qctx := query.NewLocalContext(ctx)

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())
	_, err := sender.Check(qctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})

	// The dispatch itself succeeds (our test dispatcher doesn't check context),
	// but the subcontext created inside dispatchCheck is derived from the cancelled parent.
	// In a real dispatcher, this would propagate cancellation.
	// Here we just verify the call completes without panic.
	_ = err
}

// --- Path conversion tests ---

func TestResultPathRoundTrip(t *testing.T) {
	original := &query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation: "viewer",
		Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
	}

	rp := QueryPathToResultPath(original)
	roundTripped := resultPathToQueryPath(rp)

	require.Equal(t, original.Resource, roundTripped.Resource)
	require.Equal(t, original.Relation, roundTripped.Relation)
	require.Equal(t, original.Subject, roundTripped.Subject)
	require.Nil(t, roundTripped.Caveat)
	require.Nil(t, roundTripped.Expiration)
}

func TestResultPathRoundTrip_WithExpiration(t *testing.T) {
	expTime := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	original := &query.Path{
		Resource:   query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation:   "viewer",
		Subject:    query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		Expiration: &expTime,
	}

	rp := QueryPathToResultPath(original)
	require.NotNil(t, rp.Expiration)
	require.Equal(t, timestamppb.New(expTime).AsTime(), rp.Expiration.AsTime())

	roundTripped := resultPathToQueryPath(rp)
	require.NotNil(t, roundTripped.Expiration)
	require.True(t, expTime.Equal(*roundTripped.Expiration))
}

func TestResultPathRoundTrip_WithCaveat(t *testing.T) {
	caveat := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "test_caveat",
			},
		},
	}
	original := &query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation: "viewer",
		Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		Caveat:   caveat,
	}

	rp := QueryPathToResultPath(original)
	require.NotNil(t, rp.Caveat)

	roundTripped := resultPathToQueryPath(rp)
	require.NotNil(t, roundTripped.Caveat)
	require.Equal(t, "test_caveat", roundTripped.Caveat.GetCaveat().CaveatName)
}

func TestResultPathRoundTrip_WithMetadata(t *testing.T) {
	original := &query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation: "viewer",
		Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		Metadata: map[string]any{"key": "value"},
	}

	rp := QueryPathToResultPath(original)
	require.NotNil(t, rp.Metadata)

	roundTripped := resultPathToQueryPath(rp)
	require.NotNil(t, roundTripped.Metadata)
	require.Equal(t, "value", roundTripped.Metadata["key"])
}

// --- PlanContext helper tests ---

func TestNewPlanContext(t *testing.T) {
	pc := NewPlanContext("rev1", datalayer.SchemaHash([]byte("hash")), map[string]any{"ip": "1.2.3.4"}, 10, 500)
	require.Equal(t, "rev1", pc.Revision)
	require.Equal(t, []byte("hash"), pc.SchemaHash)
	require.Equal(t, int32(10), pc.MaxRecursionDepth)
	require.Equal(t, uint64(500), pc.OptionalDatastoreLimit)
	require.NotNil(t, pc.CaveatContext)
}

func TestCaveatContextFromPlanContext(t *testing.T) {
	pc := NewPlanContext("rev1", datalayer.NoSchemaHashForTesting, map[string]any{"ip": "1.2.3.4"}, 0, 0)
	cc := CaveatContextFromPlanContext(pc)
	require.Equal(t, "1.2.3.4", cc["ip"])
}

func TestCaveatContextFromPlanContext_Nil(t *testing.T) {
	require.Nil(t, CaveatContextFromPlanContext(nil))
	require.Nil(t, CaveatContextFromPlanContext(&v1.PlanContext{}))
}

func TestPaginationLimitFromPlanContext(t *testing.T) {
	pc := NewPlanContext("rev1", datalayer.NoSchemaHashForTesting, nil, 0, 100)
	limit := PaginationLimitFromPlanContext(pc)
	require.NotNil(t, limit)
	require.Equal(t, uint64(100), *limit)
}

func TestPaginationLimitFromPlanContext_Zero(t *testing.T) {
	pc := NewPlanContext("rev1", datalayer.NoSchemaHashForTesting, nil, 0, 0)
	require.Nil(t, PaginationLimitFromPlanContext(pc))
}

// --- CheckMany dispatch tests ---

func TestDispatchExecutor_CheckManyResources_NonAliasLocal(t *testing.T) {
	receiver := &testDispatcher{}
	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	// FixedIterator (not an alias) — must delegate locally, no dispatch.
	fixed := query.NewFixedIterator(query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation: "viewer",
		Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
	})

	resources := []query.Object{
		{ObjectType: "document", ObjectID: "doc1"},
		{ObjectType: "document", ObjectID: "missing"},
	}
	out, err := sender.CheckManyResources(ctx, fixed, resources, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.Len(t, out, len(resources))
	require.NotNil(t, out[0])
	require.Nil(t, out[1])
	require.Empty(t, receiver.planCalls)
}

func TestDispatchExecutor_CheckManyResources_AliasDispatchesAndPairsByONR(t *testing.T) {
	// Receiver returns paths in arbitrary order; sender must pair to inputs by
	// resource ONR identity, not by position.
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc3", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
			},
		}},
	}
	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())
	resources := []query.Object{
		{ObjectType: "document", ObjectID: "doc1"},
		{ObjectType: "document", ObjectID: "doc2"},
		{ObjectType: "document", ObjectID: "doc3"},
	}
	out, err := sender.CheckManyResources(ctx, alias, resources, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.Len(t, out, len(resources))
	require.NotNil(t, out[0])
	require.Equal(t, "doc1", out[0].Resource.ObjectID)
	require.Nil(t, out[1], "doc2 had no response — expected nil")
	require.NotNil(t, out[2])
	require.Equal(t, "doc3", out[2].Resource.ObjectID)

	require.Len(t, receiver.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES, receiver.planCalls[0].Operation)
	require.Len(t, receiver.planCalls[0].Many, 3)
	require.Equal(t, "doc1", receiver.planCalls[0].Many[0].ObjectId)
	require.Equal(t, "doc2", receiver.planCalls[0].Many[1].ObjectId)
	require.Equal(t, "doc3", receiver.planCalls[0].Many[2].ObjectId)
}

func TestDispatchExecutor_CheckManySubjects_AliasDispatches(t *testing.T) {
	receiver := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "bob", SubjectRelation: "..."},
			},
		}},
	}
	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, 100)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())
	subjects := []query.ObjectAndRelation{
		{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		{ObjectType: "user", ObjectID: "bob", Relation: "..."},
	}
	out, err := sender.CheckManySubjects(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, subjects)
	require.NoError(t, err)
	require.Len(t, out, len(subjects))
	require.Nil(t, out[0], "alice had no response")
	require.NotNil(t, out[1])
	require.Equal(t, "bob", out[1].Subject.ObjectID)

	require.Len(t, receiver.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS, receiver.planCalls[0].Operation)
}

// chunkingDispatcher records every DispatchQueryPlan call and lets the test
// configure a per-call response, simulating chunked RPCs.
type chunkingDispatcher struct {
	planCalls []*v1.DispatchQueryPlanRequest
	respond   func(req *v1.DispatchQueryPlanRequest) []*v1.ResultPath
}

func (d *chunkingDispatcher) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{}, nil
}

func (d *chunkingDispatcher) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (d *chunkingDispatcher) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ LookupResources2Stream) error {
	return nil
}

func (d *chunkingDispatcher) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ LookupResources3Stream) error {
	return nil
}

func (d *chunkingDispatcher) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ LookupSubjectsStream) error {
	return nil
}

func (d *chunkingDispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream PlanStream) error {
	d.planCalls = append(d.planCalls, req)
	if d.respond != nil {
		paths := d.respond(req)
		if len(paths) > 0 {
			return stream.Publish(&v1.DispatchQueryPlanResponse{Paths: paths})
		}
	}
	return nil
}
func (d *chunkingDispatcher) Close() error           { return nil }
func (d *chunkingDispatcher) ReadyState() ReadyState { return ReadyState{IsReady: true} }

var _ Dispatcher = &chunkingDispatcher{}

func TestDispatchExecutor_CheckManyResources_ChunksByDispatchChunkSize(t *testing.T) {
	const chunkSize = 3
	const total = 10

	receiver := &chunkingDispatcher{
		respond: func(req *v1.DispatchQueryPlanRequest) []*v1.ResultPath {
			// Echo back every input as a match so the per-input pairing is exercised.
			out := make([]*v1.ResultPath, 0, len(req.Many))
			for _, m := range req.Many {
				out = append(out, &v1.ResultPath{
					ResourceType:    m.Namespace,
					ResourceId:      m.ObjectId,
					Relation:        "viewer",
					SubjectType:     req.Subject.Namespace,
					SubjectId:       req.Subject.ObjectId,
					SubjectRelation: req.Subject.Relation,
				})
			}
			return out
		},
	}
	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, chunkSize)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())

	resources := make([]query.Object, total)
	for i := range resources {
		resources[i] = query.Object{ObjectType: "document", ObjectID: "doc" + string(rune('a'+i))}
	}
	subject := query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}

	out, err := sender.CheckManyResources(ctx, alias, resources, subject)
	require.NoError(t, err)
	require.Len(t, out, total)
	for i, p := range out {
		require.NotNil(t, p, "output %d should be non-nil", i)
		require.Equal(t, resources[i].ObjectID, p.Resource.ObjectID)
	}

	// 10 inputs at chunkSize=3 → 4 RPCs (3,3,3,1).
	require.Len(t, receiver.planCalls, 4)
	require.Len(t, receiver.planCalls[0].Many, 3)
	require.Len(t, receiver.planCalls[1].Many, 3)
	require.Len(t, receiver.planCalls[2].Many, 3)
	require.Len(t, receiver.planCalls[3].Many, 1)
}

func TestDispatchExecutor_CheckManySubjects_ChunksByDispatchChunkSize(t *testing.T) {
	const chunkSize = 4
	const total = 9

	receiver := &chunkingDispatcher{
		respond: func(req *v1.DispatchQueryPlanRequest) []*v1.ResultPath {
			out := make([]*v1.ResultPath, 0, len(req.Many))
			for _, m := range req.Many {
				out = append(out, &v1.ResultPath{
					ResourceType:    req.Resource.Namespace,
					ResourceId:      req.Resource.ObjectId,
					Relation:        "viewer",
					SubjectType:     m.Namespace,
					SubjectId:       m.ObjectId,
					SubjectRelation: m.Relation,
				})
			}
			return out
		},
	}
	sender := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev1"}, chunkSize)
	ctx := newTestContext()

	alias := query.NewAliasIterator("", "viewer", query.NewFixedIterator())
	subjects := make([]query.ObjectAndRelation, total)
	for i := range subjects {
		subjects[i] = query.ObjectAndRelation{ObjectType: "user", ObjectID: "u" + string(rune('a'+i)), Relation: "..."}
	}
	resource := query.Object{ObjectType: "document", ObjectID: "doc1"}

	out, err := sender.CheckManySubjects(ctx, alias, resource, subjects)
	require.NoError(t, err)
	require.Len(t, out, total)
	for i, p := range out {
		require.NotNil(t, p, "output %d should be non-nil", i)
		require.Equal(t, subjects[i].ObjectID, p.Subject.ObjectID)
	}

	// 9 inputs at chunkSize=4 → 3 RPCs (4,4,1).
	require.Len(t, receiver.planCalls, 3)
	require.Len(t, receiver.planCalls[0].Many, 4)
	require.Len(t, receiver.planCalls[1].Many, 4)
	require.Len(t, receiver.planCalls[2].Many, 1)
}
