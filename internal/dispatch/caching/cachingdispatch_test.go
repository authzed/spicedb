package caching

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type checkRequest struct {
	start             string
	goal              string
	atRevision        decimal.Decimal
	depthRequired     uint32
	depthRemaining    uint32
	expectPassthrough bool
}

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func TestMaxDepthCaching(t *testing.T) {
	start1 := "document:doc1#read"
	start2 := "document:doc2#read"
	user1 := "user:user1#..."
	user2 := "user:user2#..."

	testCases := []struct {
		name   string
		script []checkRequest
	}{
		{"single request", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
		}},
		{"two requests, hit", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.Zero, 1, 50, false},
		}},
		{"many requests, hit", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start1, user1, decimal.Zero, 1, 50, false},
		}},
		{"multiple keys", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start2, user2, decimal.Zero, 1, 50, true},
		}},
		{"same object, different revisions miss", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.NewFromInt(50), 1, 50, true},
		}},
		{"interleaved objects, hit", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start2, user2, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start2, user2, decimal.Zero, 1, 50, false},
		}},
		{"insufficient depth", []checkRequest{
			{start1, user1, decimal.Zero, 21, 50, true},
			{start1, user1, decimal.Zero, 21, 20, true},
		}},
		{"sufficient depth", []checkRequest{
			{start1, user1, decimal.Zero, 1, 40, true},
			{start1, user1, decimal.Zero, 1, 50, false},
		}},
		{"updated cached depth", []checkRequest{
			{start1, user1, decimal.Zero, 21, 50, true},
			{start1, user1, decimal.Zero, 21, 40, false},
			{start1, user1, decimal.Zero, 21, 20, true},
			{start1, user1, decimal.Zero, 21, 50, false},
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			delegate := delegateDispatchMock{&mock.Mock{}}

			for _, step := range tc.script {
				if step.expectPassthrough {
					parsed, err := tuple.ParseONR(step.start)
					require.NoError(err)

					delegate.On("DispatchCheck", &v1.DispatchCheckRequest{
						ResourceRelation: RR(parsed.ObjectType, parsed.Relation),
						ResourceIds:      []string{parsed.ObjectID},
						Subject:          tuple.MustParseSubjectONR(step.goal).ToCoreONR(),
						Metadata: &v1.ResolverMeta{
							AtRevision:     step.atRevision.String(),
							DepthRemaining: step.depthRemaining,
							SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
						},
					}).Return(&v1.DispatchCheckResponse{
						ResultsByResourceId: map[string]*v1.ResourceCheckResult{
							parsed.ObjectID: {
								Membership: v1.ResourceCheckResult_MEMBER,
							},
						},
						Metadata: &v1.ResponseMeta{
							DispatchCount: 1,
							DepthRequired: step.depthRequired,
						},
					}, nil).Times(1)
				}
			}

			dispatch, err := NewCachingDispatcher(DispatchTestCache(t), false, "", nil)
			dispatch.SetDelegate(delegate)
			require.NoError(err)
			defer dispatch.Close()

			for _, step := range tc.script {
				parsed, err := tuple.ParseONR(step.start)
				require.NoError(err)

				resp, err := dispatch.DispatchCheck(t.Context(), &v1.DispatchCheckRequest{
					ResourceRelation: RR(parsed.ObjectType, parsed.Relation),
					ResourceIds:      []string{parsed.ObjectID},
					Subject:          tuple.MustParseSubjectONR(step.goal).ToCoreONR(),
					Metadata: &v1.ResolverMeta{
						AtRevision:     step.atRevision.String(),
						DepthRemaining: step.depthRemaining,
						SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
					},
				})
				require.NoError(err)
				require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId[parsed.ObjectID].Membership)

				// We have to sleep a while to let the cache converge
				time.Sleep(10 * time.Millisecond)
			}

			delegate.AssertExpectations(t)
		})
	}
}

func TestConcurrentDebugInfoAccess(t *testing.T) {
	require := require.New(t)

	delegate := delegateDispatchMock{&mock.Mock{}}

	originalReq := &v1.DispatchCheckRequest{
		ResourceIds: []string{"original"},
		Subject: &core.ObjectAndRelation{
			Relation: "original",
		},
	}

	// Have the delegate return one request object for all calls
	delegate.On("DispatchCheck", mock.Anything).
		Return(&v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: 1,
				DebugInfo:     &v1.DebugInformation{Check: &v1.CheckDebugTrace{Request: originalReq}},
			},
		}, nil)

	dispatcher, err := NewCachingDispatcher(DispatchTestCache(t), false, "", nil)
	require.NoError(err)
	dispatcher.SetDelegate(delegate)
	t.Cleanup(func() {
		_ = dispatcher.Close()
	})

	// Spawn multiple goroutines that create requests with slice views of the same backing array
	// This simulates what happens in ComputeBulkCheck with ForEachChunkUntil
	const numGoroutines = 1000
	errors := make(chan error, numGoroutines)

	var wg sync.WaitGroup
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()

			request := &v1.DispatchCheckRequest{
				ResourceRelation: RR("document", "viewer"),
				ResourceIds:      []string{"doc1"},
				Subject:          tuple.MustParseSubjectONR("user:alice").ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     decimal.Zero.String(),
					DepthRemaining: 50,
					SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
				},
				Debug: v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING,
			}

			resp, err := dispatcher.DispatchCheck(t.Context(), request)
			if err != nil {
				errors <- err
				return
			}

			assert.NotNil(t, resp.GetMetadata().GetDebugInfo().GetCheck().GetRequest())

			// we mutate the response to prove that it's not shared across goroutines
			resp.GetMetadata().GetDebugInfo().GetCheck().GetRequest().Subject.Relation = "modified"
			resp.GetMetadata().GetDebugInfo().GetCheck().GetRequest().ResourceIds = []string{"modified"}
		}()
	}

	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(err)
	}
}

// planDispatchMock is a minimal dispatch.Dispatcher used by the caching plan-metrics
// test. It publishes a single DispatchQueryPlanResponse carrying one ResultPath so
// the caching layer's cache-write path is exercised.
type planDispatchMock struct {
	delegateDispatchMock
	calls atomic.Uint64
}

func (p *planDispatchMock) DispatchQueryPlan(_ *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	p.calls.Add(1)
	return stream.Publish(&v1.DispatchQueryPlanResponse{
		Metadata: &v1.ResponseMeta{DispatchCount: 1},
		Paths: []*v1.ResultPath{{
			ResourceType: "document",
			ResourceId:   "doc1",
			Relation:     "viewer",
		}},
	})
}

func sumOperationCounter(t *testing.T, gatherer prometheus.Gatherer, metricName, operationLabel string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != metricName {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			matched := operationLabel == ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "operation" && l.GetValue() == operationLabel {
					matched = true
				}
			}
			if matched {
				sum += m.GetCounter().GetValue()
			}
		}
		return sum
	}
	return 0
}

func sumPlainCounter(t *testing.T, gatherer prometheus.Gatherer, metricName string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != metricName {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
		return sum
	}
	return 0
}

func TestDispatchQueryPlanRecordsCachingMetrics(t *testing.T) {
	// Use a unique subsystem so we don't collide with other tests in the global
	// default Prometheus registry.
	subsystem := fmt.Sprintf("test_caching_%d", time.Now().UnixNano())

	dispatcher, err := NewCachingDispatcher(DispatchTestCache(t), true, subsystem, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dispatcher.Close() })

	delegate := &planDispatchMock{delegateDispatchMock: delegateDispatchMock{&mock.Mock{}}}
	dispatcher.SetDelegate(delegate)

	req := &v1.DispatchQueryPlanRequest{
		Operation:    v1.PlanOperation_PLAN_OPERATION_CHECK,
		CanonicalKey: "document#viewer",
		Resource:     tuple.ONRStringToCore("document", "doc1", "viewer"),
		Subject:      tuple.ONRStringToCore("user", "alice", "..."),
		PlanContext: &v1.PlanContext{
			Revision:   "1",
			SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	// First call: cache miss; bumps query_plan_total.
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
	require.NoError(t, dispatcher.DispatchQueryPlan(req, stream))
	require.Len(t, stream.Results(), 1)

	// Give the cache a chance to converge before the second call.
	time.Sleep(10 * time.Millisecond)

	// Second call: cache hit; bumps both query_plan_total and query_plan_from_cache_total.
	stream2 := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
	require.NoError(t, dispatcher.DispatchQueryPlan(req, stream2))
	require.Len(t, stream2.Results(), 1)

	gatherer := prometheus.DefaultGatherer

	totalCheck := sumOperationCounter(t, gatherer, "spicedb_"+subsystem+"_query_plan_total", "check")
	require.InEpsilon(t, float64(2), totalCheck, 1e-9, "query_plan_total{operation=check} should bump for each plan dispatch")

	fromCacheCheck := sumOperationCounter(t, gatherer, "spicedb_"+subsystem+"_query_plan_from_cache_total", "check")
	require.InEpsilon(t, float64(1), fromCacheCheck, 1e-9, "query_plan_from_cache_total{operation=check} should bump only on cache hit")

	// Ensure the plan path no longer inflates the classic DispatchCheck counter.
	checkTotal := sumPlainCounter(t, gatherer, "spicedb_"+subsystem+"_check_total")
	require.Zero(t, checkTotal, "check_total must not be incremented from the plan path")

	// The delegate is only consulted on the cache miss.
	require.Equal(t, uint64(1), delegate.calls.Load(), "delegate should only be called once; second request must be served from cache")
}

type delegateDispatchMock struct {
	*mock.Mock
}

func (ddm delegateDispatchMock) DispatchCheck(_ context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	args := ddm.Called(req)
	return args.Get(0).(*v1.DispatchCheckResponse), args.Error(1)
}

func (ddm delegateDispatchMock) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (ddm delegateDispatchMock) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ dispatch.LookupResources2Stream) error {
	return nil
}

func (ddm delegateDispatchMock) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ dispatch.LookupResources3Stream) error {
	return nil
}

func (ddm delegateDispatchMock) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return nil
}

func (ddm delegateDispatchMock) DispatchQueryPlan(_ *v1.DispatchQueryPlanRequest, _ dispatch.PlanStream) error {
	return nil
}

func (ddm delegateDispatchMock) LookupPlanCheck(_ context.Context, _ dispatch.PlanCheckLookup) (*v1.ResultPath, bool, error) {
	return nil, false, nil
}

func (ddm delegateDispatchMock) Close() error {
	return nil
}

func (ddm delegateDispatchMock) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{IsReady: true}
}

var _ dispatch.Dispatcher = &delegateDispatchMock{}
