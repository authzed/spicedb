package singleflight

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSingleFlightDispatcher(t *testing.T) {
	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "bar"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				RequestId:  "first",
			},
		})
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "bar"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				RequestId:  "second",
			},
		})
		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "bar"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				RequestId:  "third",
			},
		})

		wg.Done()
	}()
	go func() {
		_, _ = disp.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "baz"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
			},
		})
		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(2), called.Load())
}

func TestSingleFlightDispatcherCancelation(t *testing.T) {
	var called atomic.Uint64
	run := make(chan struct{}, 1)
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
		run <- struct{}{}
	}
	disp := New(mockDispatcher{f: f}, &keys.DirectKeyHandler{})

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "bar"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				RequestId:  "first",
			},
		})
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "bar"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				RequestId:  "second",
			},
		})
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := disp.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "bar"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				RequestId:  "third",
			},
		})
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()

	wg.Wait()
	<-run
	require.Equal(t, uint64(1), called.Load())
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
