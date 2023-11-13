package singleflight

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	maxDefaultDepth        = 50
	defaultBloomFilterSize = maxDefaultDepth + 1
)

func TestSingleFlightDispatcher(t *testing.T) {
	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RelationReference("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: MustNewTraversalBloomFilter(defaultBloomFilterSize),
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req)
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req)
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req)
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
	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	keyHandler := &keys.DirectKeyHandler{}
	disp := New(mockDispatcher{f: f}, keyHandler)

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RelationReference("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: MustNewTraversalBloomFilter(defaultBloomFilterSize),
		},
	}

	// we simulate the request above being already part of the traversal path,
	// so that the dispatcher detects a loop and does not singleflight
	req.Metadata.TraversalBloom = bloomFilterForRequest(t, keyHandler, req)

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req)
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req)
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), req)

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
		ResourceRelation: tuple.RelationReference("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     "1234",
			TraversalBloom: MustNewTraversalBloomFilter(defaultBloomFilterSize),
		},
	}

	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req)
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req)
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, req)
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()

	wg.Wait()
	<-run
	require.Equal(t, uint64(1), called.Load())
}

func bloomFilterForRequest(t *testing.T, keyHandler *keys.DirectKeyHandler, req *v1.DispatchCheckRequest) []byte {
	t.Helper()

	bloomFilter := bloom.NewWithEstimates(defaultBloomFilterSize, defaultFalsePositiveRate)
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
	return &v1.DispatchExpandResponse{}, nil
}

func (m mockDispatcher) DispatchReachableResources(_ *v1.DispatchReachableResourcesRequest, _ dispatch.ReachableResourcesStream) error {
	return nil
}

func (m mockDispatcher) DispatchLookupResources(_ *v1.DispatchLookupResourcesRequest, _ dispatch.LookupResourcesStream) error {
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