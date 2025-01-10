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
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
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
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		anotherReq := req.CloneVT()
		anotherReq.ResourceIds = []string{"foo", "baz"}
		_, _ = disp.DispatchCheck(context.Background(), anotherReq)
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
		},
	}

	// we simulate the request above being already part of the traversal path,
	// so that the dispatcher detects a loop and does not singleflight
	req.Metadata.TraversalBloom = bloomFilterForRequest(t, keyHandler, req)

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())

		wg.Done()
	}()
	go func() {
		differentReq := req.CloneVT()
		differentReq.ResourceIds = []string{"foo", "baz"}
		_, _ = disp.DispatchCheck(context.Background(), differentReq)
		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(4), called.Load(), "should have dispatched %d calls but did %d", uint64(4), called.Load())
	assertCounterWithLabel(t, reg, 2, "spicedb_dispatch_single_flight_total", "loop")
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
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())

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
		},
	}

	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req.CloneVT())
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req.CloneVT())
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req.CloneVT())
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()

	wg.Wait()
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
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchExpand(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchExpand(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchExpand(context.Background(), req.CloneVT())
		wg.Done()
	}()
	go func() {
		anotherReq := req.CloneVT()
		anotherReq.ResourceAndRelation.ObjectId = "baz"
		_, _ = disp.DispatchExpand(context.Background(), anotherReq)
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
		},
	}

	_, _ = disp.DispatchCheck(context.Background(), req.CloneVT())

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
		},
	}

	_, _ = disp.DispatchExpand(context.Background(), req.CloneVT())

	require.Equal(t, uint64(1), called.Load(), "should have dispatched %d calls but did %d", uint64(1), called.Load())
	assertCounterWithLabel(t, reg, 1, "spicedb_dispatch_single_flight_total", "missing")
}

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
	key, err := keyHandler.CheckDispatchKey(context.Background(), req)
	require.NoError(t, err)
	stringKey := hex.EncodeToString(key)
	bloomFilter = bloomFilter.AddString(stringKey)
	binaryBloom, err := bloomFilter.MarshalBinary()
	require.NoError(t, err)

	return binaryBloom
}

type mockDispatcher struct {
	f func()
}

func (m mockDispatcher) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	m.f()
	return &v1.DispatchCheckResponse{}, nil
}

func (m mockDispatcher) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	m.f()
	return &v1.DispatchExpandResponse{}, nil
}

func (m mockDispatcher) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ dispatch.LookupResources2Stream) error {
	return nil
}

func (m mockDispatcher) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return nil
}

func (m mockDispatcher) Close() error {
	return nil
}

func (m mockDispatcher) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{}
}
