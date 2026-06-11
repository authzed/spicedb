package singleflight

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultBloomFilterSize = 50

func TestSingleFlightDispatcher(t *testing.T) {
	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ONRStringToCore("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: v1.MustNewTraversalBloomFilter(defaultBloomFilterSize),
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		resp1, err := disp.DispatchCheck(t.Context(), req.CloneVT())
		assert.NoError(t, err)
		// this goroutine mutates the response; other goroutines that read should be unaffected
		resp1.GetMetadata().GetDebugInfo().GetCheck().GetSubProblems()[0].IsCachedResult = false
		wg.Done()
	}()
	go func() {
		resp2, _ := disp.DispatchCheck(t.Context(), req.CloneVT())
		// this goroutine reads the response
		t.Log(resp2)
		wg.Done()
	}()
	go func() {
		resp3, _ := disp.DispatchCheck(t.Context(), req.CloneVT())
		t.Log(resp3)
		wg.Done()
	}()
	go func() {
		anotherReq := req.CloneVT()
		anotherReq.ResourceIds = []string{"foo", "baz"}
		resp4, _ := disp.DispatchCheck(t.Context(), anotherReq)
		t.Log(resp4)
		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(2), called.Load(), "should have dispatched %d calls but did %d", uint64(2), called.Load())
}

func TestSingleFlightDispatcherDetectsLoop(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	keyHandler := &keys.DirectKeyHandler{}
	disp := New(mockDispatcher{f: f}, keyHandler)

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ONRStringToCore("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: v1.MustNewTraversalBloomFilter(defaultBloomFilterSize),
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	// we simulate the request above being already part of the traversal path,
	// so that the dispatcher detects a loop and does not singleflight
	req.Metadata.TraversalBloom = bloomFilterForRequest(t, keyHandler, req)

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())

		wg.Done()
	}()
	go func() {
		differentReq := req.CloneVT()
		differentReq.ResourceIds = []string{"foo", "baz"}
		_, _ = disp.DispatchCheck(t.Context(), differentReq)
		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(4), called.Load(), "should have dispatched %d calls but did %d", uint64(4), called.Load())
	assertCounterWithLabel(t, reg, 2, "spicedb_dispatch_single_flight_total", "loop")
}

// TestSingleFlightDispatcherDoesNotShareDebugChecks ensures debug-enabled checks
// are dispatched directly rather than sharing a flight with otherwise-identical
// non-debug checks. The dispatch key ignores req.Debug, so sharing would leak the
// leader's debug info to a non-debug follower (the cause of #3159) or deny a debug
// follower its trace.
func TestSingleFlightDispatcherDoesNotShareDebugChecks(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	// The delegate returns debug info only when the request asked for it, so the
	// response carried back to each caller reveals whether a flight was shared
	// across debug levels.
	delegate := &debugAwareDispatcher{delay: 100 * time.Millisecond}
	disp := New(delegate, &keys.DirectKeyHandler{})

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ONRStringToCore("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: v1.MustNewTraversalBloomFilter(defaultBloomFilterSize),
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	var mu sync.Mutex
	var nonDebugResponses, debugResponses []*v1.DispatchCheckResponse

	wg := sync.WaitGroup{}
	wg.Add(4)
	// Two non-debug checks should share a single flight.
	for range 2 {
		go func() {
			resp, _ := disp.DispatchCheck(t.Context(), req.CloneVT())
			mu.Lock()
			nonDebugResponses = append(nonDebugResponses, resp)
			mu.Unlock()
			wg.Done()
		}()
	}
	// Two debug-enabled checks, identical key, must each dispatch directly. Cover
	// both debug levels: WithTracing maps to basic debugging, while trace debugging
	// is the deeper variant; the bypass keys on != NO_DEBUG so both must apply.
	for _, debug := range []v1.DispatchCheckRequest_DebugSetting{
		v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING,
		v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING,
	} {
		go func() {
			debugReq := req.CloneVT()
			debugReq.Debug = debug
			resp, _ := disp.DispatchCheck(t.Context(), debugReq)
			mu.Lock()
			debugResponses = append(debugResponses, resp)
			mu.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	// One dispatch for the shared non-debug pair, plus one per debug check.
	require.Equal(t, uint64(3), delegate.called.Load(), "should have dispatched 3 calls but did %d", delegate.called.Load())
	// Two label series: shared=true (the shared non-debug pair) and shared=debug.
	assertCounterWithLabel(t, reg, 2, "spicedb_dispatch_single_flight_total", "debug")

	// A non-debug caller must never receive debug info, even when racing an
	// identical debug check; a debug caller must always receive its own.
	for _, resp := range nonDebugResponses {
		require.Nil(t, resp.GetMetadata().GetDebugInfo(), "non-debug caller received leaked debug info")
	}
	for _, resp := range debugResponses {
		require.NotNil(t, resp.GetMetadata().GetDebugInfo(), "debug caller lost its trace")
	}
}

// this test makes sure that bloom filter information is carried from dispatcher to dispatcher
func TestSingleFlightDispatcherDetectsLoopThroughDelegate(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	keyHandler := &keys.DirectKeyHandler{}
	// we simulate an actual dispatch-chain loop by nesting 2 singleflight dispatchers
	disp := New(New(mockDispatcher{f: f}, keyHandler), keyHandler)

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ONRStringToCore("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: v1.MustNewTraversalBloomFilter(defaultBloomFilterSize),
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())

		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(1), called.Load(), "should have dispatched %d calls but did %d", uint64(1), called.Load())
	assertCounterWithLabel(t, reg, 2, "spicedb_dispatch_single_flight_total", "loop")
}

func TestSingleFlightDispatcherCancelation(t *testing.T) {
	var called atomic.Uint64
	run := make(chan struct{}, 1)
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
		run <- struct{}{}
	}

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ONRStringToCore("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: v1.MustNewTraversalBloomFilter(defaultBloomFilterSize),
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})
	wg := sync.WaitGroup{}
	wg.Add(3)
	errs := make(chan error, 3)

	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req.CloneVT())
		errs <- err
		wg.Done()
	}()
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req.CloneVT())
		errs <- err
		wg.Done()
	}()
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req.CloneVT())
		errs <- err
		wg.Done()
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	<-run
	require.Equal(t, uint64(1), called.Load())
}

func TestSingleFlightDispatcherExpand(t *testing.T) {
	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	req := &v1.DispatchExpandRequest{
		ResourceAndRelation: tuple.ONRStringToCore("document", "foo", "view"),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: v1.MustNewTraversalBloomFilter(defaultBloomFilterSize),
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchExpand(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchExpand(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchExpand(t.Context(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		anotherReq := req.CloneVT()
		anotherReq.ResourceAndRelation.ObjectId = "baz"
		_, _ = disp.DispatchExpand(t.Context(), anotherReq)
		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(2), called.Load(), "should have dispatched %d calls but did %d", uint64(2), called.Load())
}

func TestSingleFlightDispatcherCheckBypassesIfMissingBloomFiler(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	var called atomic.Uint64
	f := func() {
		called.Add(1)
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ONRStringToCore("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
			SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	_, _ = disp.DispatchCheck(t.Context(), req.CloneVT())

	require.Equal(t, uint64(1), called.Load(), "should have dispatched %d calls but did %d", uint64(1), called.Load())
	assertCounterWithLabel(t, reg, 1, "spicedb_dispatch_single_flight_total", "missing")
}

func TestSingleFlightDispatcherExpandBypassesIfMissingBloomFiler(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	var called atomic.Uint64
	f := func() {
		called.Add(1)
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	req := &v1.DispatchExpandRequest{
		ResourceAndRelation: tuple.ONRStringToCore("document", "foo", "view"),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
			SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
		},
	}

	_, _ = disp.DispatchExpand(t.Context(), req.CloneVT())

	require.Equal(t, uint64(1), called.Load(), "should have dispatched %d calls but did %d", uint64(1), called.Load())
	assertCounterWithLabel(t, reg, 1, "spicedb_dispatch_single_flight_total", "missing")
}

func TestDispatchQueryPlanRecordsSingleflightMetric(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	// Non-CHECK operations are passthrough — verify the metric fires with the
	// passthrough label.
	disp := New(mockDispatcher{f: func() {}}, &keys.DirectKeyHandler{})

	req := &v1.DispatchQueryPlanRequest{
		Operation: v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES,
	}
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())

	err := disp.DispatchQueryPlan(req, stream)
	require.NoError(t, err)

	assertCounterWithLabel(t, reg, 1, "spicedb_dispatch_single_flight_total", "DispatchQueryPlan")
}

func TestDispatchQueryPlanCheckCoalescesConcurrentCalls(t *testing.T) {
	singleFlightCount = prometheus.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	reg := registerMetricInGatherer(singleFlightCount)

	var called atomic.Uint64
	mock := planMockDispatcher{
		f: func() {
			// Hold the singleflight long enough for the second caller to
			// attach to the in-flight group.
			time.Sleep(100 * time.Millisecond)
			called.Add(1)
		},
	}
	disp := New(mock, &keys.DirectKeyHandler{})

	req := &v1.DispatchQueryPlanRequest{
		Operation: v1.PlanOperation_PLAN_OPERATION_CHECK,
		Resource:  tuple.ONRStringToCore("document", "doc1", "viewer"),
		Subject:   tuple.ONRStringToCore("user", "alice", "..."),
		PlanContext: &v1.PlanContext{
			Revision:       "1234",
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
			InProgressKeys: []string{"document#viewer"},
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
		assert.NoError(t, disp.DispatchQueryPlan(req.CloneVT(), stream))
		assert.Len(t, stream.Results(), 1)
	}()
	go func() {
		defer wg.Done()
		// Stagger slightly so the first goroutine wins the group leadership.
		time.Sleep(10 * time.Millisecond)
		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
		assert.NoError(t, disp.DispatchQueryPlan(req.CloneVT(), stream))
		assert.Len(t, stream.Results(), 1)
	}()
	wg.Wait()

	require.Equal(t, uint64(1), called.Load(), "delegate should be invoked once; the second caller should share the result")
	// Exactly one {method=DispatchQueryPlan, shared=true} series should exist
	// with both callers credited to it.
	assertCounterWithLabel(t, reg, 1, "spicedb_dispatch_single_flight_total", "true")
	require.InEpsilon(t, float64(2), counterValueWithLabels(t, reg, "spicedb_dispatch_single_flight_total", map[string]string{
		"method": "DispatchQueryPlan",
		"shared": "true",
	}), 0)
}

func counterValueWithLabels(t *testing.T, gatherer prometheus.Gatherer, metricName string, want map[string]string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for _, l := range m.GetLabel() {
				if v, ok := want[l.GetName()]; ok && v != l.GetValue() {
					match = false
					break
				}
			}
			if match {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// planMockDispatcher is a dispatcher whose DispatchQueryPlan publishes a single
// response carrying one ResultPath. It is used to exercise singleflight
// coalescing for plan-check.
type planMockDispatcher struct {
	f func()
}

func (m planMockDispatcher) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, nil
}

func (m planMockDispatcher) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (m planMockDispatcher) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ dispatch.LookupResources2Stream) error {
	return nil
}

func (m planMockDispatcher) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ dispatch.LookupResources3Stream) error {
	return nil
}

func (m planMockDispatcher) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return nil
}

func (m planMockDispatcher) DispatchQueryPlan(_ *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	m.f()
	return stream.Publish(&v1.DispatchQueryPlanResponse{
		Metadata: &v1.ResponseMeta{DispatchCount: 1},
		Paths: []*v1.ResultPath{{
			ResourceType: "document",
			ResourceId:   "doc1",
			Relation:     "viewer",
		}},
	})
}

func (m planMockDispatcher) LookupPlanCheck(_ context.Context, _ dispatch.PlanCheckLookup) (*v1.ResultPath, bool, error) {
	return nil, false, nil
}
func (m planMockDispatcher) Close() error                    { return nil }
func (m planMockDispatcher) ReadyState() dispatch.ReadyState { return dispatch.ReadyState{} }

func registerMetricInGatherer(collector prometheus.Collector) prometheus.Gatherer {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collector)

	return reg
}

func assertCounterWithLabel(t *testing.T, gatherer prometheus.Gatherer, expectedMetricsCount int, metricName, labelName string) {
	t.Helper()

	metrics, err := gatherer.Gather()
	require.NoError(t, err)

	var mf *promclient.MetricFamily
	for _, metric := range metrics {
		if metric.GetName() == metricName {
			mf = metric
		}
	}

	found := false
	require.Len(t, mf.GetMetric(), expectedMetricsCount)
	for _, metric := range mf.GetMetric() {
		for _, label := range metric.Label {
			if *label.Value == labelName {
				found = true
			}
		}
	}

	require.True(t, found, "didn't find counter with label %s", labelName)
}

func bloomFilterForRequest(t *testing.T, keyHandler *keys.DirectKeyHandler, req *v1.DispatchCheckRequest) []byte {
	t.Helper()

	bloomFilter := bloom.NewWithEstimates(defaultBloomFilterSize, 0.001)
	key, err := keyHandler.CheckDispatchKey(t.Context(), req)
	require.NoError(t, err)
	stringKey := hex.EncodeToString(key)
	bloomFilter = bloomFilter.AddString(stringKey)
	binaryBloom, err := bloomFilter.MarshalBinary()
	require.NoError(t, err)

	return binaryBloom
}

// debugAwareDispatcher returns debug info only when the request asked for it,
// so callers can assert that a non-debug caller never receives debug info from a
// flight shared with a debug caller. Only DispatchCheck is exercised.
type debugAwareDispatcher struct {
	dispatch.Dispatcher
	delay  time.Duration
	called atomic.Uint64
}

func (d *debugAwareDispatcher) DispatchCheck(_ context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	time.Sleep(d.delay)
	d.called.Add(1)

	resp := &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}
	if req.Debug != v1.DispatchCheckRequest_NO_DEBUG {
		resp.Metadata.DebugInfo = &v1.DebugInformation{Check: &v1.CheckDebugTrace{}}
	}
	return resp, nil
}

type mockDispatcher struct {
	f func()
}

func (m mockDispatcher) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	m.f()
	return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DebugInfo: &v1.DebugInformation{Check: &v1.CheckDebugTrace{
		SubProblems: []*v1.CheckDebugTrace{{IsCachedResult: true}},
	}}}}, nil
}

func (m mockDispatcher) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	m.f()
	return &v1.DispatchExpandResponse{}, nil
}

func (m mockDispatcher) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ dispatch.LookupResources2Stream) error {
	return nil
}

func (m mockDispatcher) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ dispatch.LookupResources3Stream) error {
	return nil
}

func (m mockDispatcher) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return nil
}

func (m mockDispatcher) DispatchQueryPlan(_ *v1.DispatchQueryPlanRequest, _ dispatch.PlanStream) error {
	return nil
}

func (m mockDispatcher) LookupPlanCheck(_ context.Context, _ dispatch.PlanCheckLookup) (*v1.ResultPath, bool, error) {
	return nil, false, nil
}

func (m mockDispatcher) Close() error {
	return nil
}

func (m mockDispatcher) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{}
}
