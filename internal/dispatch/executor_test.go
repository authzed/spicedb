package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	td := &testDispatcher{
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

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	// AliasIterator wrapping a FixedIterator — should trigger dispatch
	inner := query.NewFixedIterator()
	alias := query.NewAliasIterator("viewer", inner)

	path, err := exec.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "document", path.Resource.ObjectType)
	require.Equal(t, "doc1", path.Resource.ObjectID)
	require.Equal(t, "alice", path.Subject.ObjectID)

	// Verify dispatch was called
	require.Len(t, td.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_CHECK, td.planCalls[0].Operation)
	require.Equal(t, "rev1", td.planCalls[0].PlanContext.Revision)
}

func TestDispatchExecutor_Check_NonAliasLocal(t *testing.T) {
	td := &testDispatcher{}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	// FixedIterator (not an alias) — should delegate locally, no dispatch
	fixed := query.NewFixedIterator(query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
		Relation: "viewer",
		Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
	})

	path, err := exec.Check(ctx, fixed, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Equal(t, "alice", path.Subject.ObjectID)

	// Verify NO dispatch was called
	require.Empty(t, td.planCalls)
}

func TestDispatchExecutor_Check_AliasNoResult(t *testing.T) {
	td := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{}, // empty = no permission
		}},
	}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	alias := query.NewAliasIterator("viewer", query.NewFixedIterator())

	path, err := exec.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.Nil(t, path)
	require.Len(t, td.planCalls, 1)
}

func TestDispatchExecutor_IterSubjects_AliasDispatches(t *testing.T) {
	td := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "bob", SubjectRelation: "..."},
			},
		}},
	}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	alias := query.NewAliasIterator("viewer", query.NewFixedIterator())

	pathSeq, err := exec.IterSubjects(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.Equal(t, "alice", paths[0].Subject.ObjectID)
	require.Equal(t, "bob", paths[1].Subject.ObjectID)

	require.Len(t, td.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS, td.planCalls[0].Operation)
}

func TestDispatchExecutor_IterSubjects_NonAliasLocal(t *testing.T) {
	td := &testDispatcher{}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
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

	pathSeq, err := exec.IterSubjects(ctx, fixed, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.Empty(t, td.planCalls)
}

func TestDispatchExecutor_IterResources_AliasDispatches(t *testing.T) {
	td := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "doc1", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
				{ResourceType: "document", ResourceId: "doc2", Relation: "viewer", SubjectType: "user", SubjectId: "alice", SubjectRelation: "..."},
			},
		}},
	}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	alias := query.NewAliasIterator("viewer", query.NewFixedIterator())

	pathSeq, err := exec.IterResources(ctx, alias, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 2)
	require.Equal(t, "doc1", paths[0].Resource.ObjectID)
	require.Equal(t, "doc2", paths[1].Resource.ObjectID)

	require.Len(t, td.planCalls, 1)
	require.Equal(t, v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES, td.planCalls[0].Operation)
}

func TestDispatchExecutor_IterResources_NonAliasLocal(t *testing.T) {
	td := &testDispatcher{}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	fixed := query.NewFixedIterator(
		query.Path{
			Resource: query.Object{ObjectType: "document", ObjectID: "doc1"},
			Relation: "viewer",
			Subject:  query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
	)

	pathSeq, err := exec.IterResources(ctx, fixed, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Empty(t, td.planCalls)
}

func TestDispatchExecutor_StreamBatching(t *testing.T) {
	// Multiple streamed responses should be collected correctly
	td := &testDispatcher{
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

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	ctx := newTestContext()

	alias := query.NewAliasIterator("viewer", query.NewFixedIterator())

	pathSeq, err := exec.IterResources(ctx, alias, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, query.NoObjectFilter())
	require.NoError(t, err)

	paths, err := query.CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 3)
	require.Equal(t, "doc1", paths[0].Resource.ObjectID)
	require.Equal(t, "doc2", paths[1].Resource.ObjectID)
	require.Equal(t, "doc3", paths[2].Resource.ObjectID)
}

func TestDispatchExecutor_PlanContextForwarded(t *testing.T) {
	td := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{Paths: []*v1.ResultPath{}}},
	}

	pc := &v1.PlanContext{
		Revision:               "rev42",
		MaxRecursionDepth:      5,
		OptionalDatastoreLimit: 100,
	}
	exec := NewDispatchExecutor(td, pc)
	ctx := newTestContext()

	alias := query.NewAliasIterator("viewer", query.NewFixedIterator())
	_, _ = exec.Check(ctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})

	require.Len(t, td.planCalls, 1)
	require.Equal(t, "rev42", td.planCalls[0].PlanContext.Revision)
	require.Equal(t, int32(5), td.planCalls[0].PlanContext.MaxRecursionDepth)
	require.Equal(t, uint64(100), td.planCalls[0].PlanContext.OptionalDatastoreLimit)
}

func TestDispatchExecutor_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	td := &testDispatcher{
		planResponses: []*v1.DispatchQueryPlanResponse{{Paths: []*v1.ResultPath{}}},
	}

	exec := NewDispatchExecutor(td, &v1.PlanContext{Revision: "rev1"})
	qctx := query.NewLocalContext(ctx)

	alias := query.NewAliasIterator("viewer", query.NewFixedIterator())
	_, err := exec.Check(qctx, alias, query.Object{ObjectType: "document", ObjectID: "doc1"}, query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})

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
	pc := NewPlanContext("rev1", map[string]any{"ip": "1.2.3.4"}, 10, 500)
	require.Equal(t, "rev1", pc.Revision)
	require.Equal(t, int32(10), pc.MaxRecursionDepth)
	require.Equal(t, uint64(500), pc.OptionalDatastoreLimit)
	require.NotNil(t, pc.CaveatContext)
}

func TestCaveatContextFromPlanContext(t *testing.T) {
	pc := NewPlanContext("rev1", map[string]any{"ip": "1.2.3.4"}, 0, 0)
	cc := CaveatContextFromPlanContext(pc)
	require.Equal(t, "1.2.3.4", cc["ip"])
}

func TestCaveatContextFromPlanContext_Nil(t *testing.T) {
	require.Nil(t, CaveatContextFromPlanContext(nil))
	require.Nil(t, CaveatContextFromPlanContext(&v1.PlanContext{}))
}

func TestPaginationLimitFromPlanContext(t *testing.T) {
	pc := NewPlanContext("rev1", nil, 0, 100)
	limit := PaginationLimitFromPlanContext(pc)
	require.NotNil(t, limit)
	require.Equal(t, uint64(100), *limit)
}

func TestPaginationLimitFromPlanContext_Zero(t *testing.T) {
	pc := NewPlanContext("rev1", nil, 0, 0)
	require.Nil(t, PaginationLimitFromPlanContext(pc))
}
