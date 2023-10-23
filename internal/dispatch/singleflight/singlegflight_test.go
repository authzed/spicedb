package singleflight

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestHashForDispatchCheck(t *testing.T) {
	for _, tt := range []struct {
		Message      *v1.DispatchCheckRequest
		ExpectedHash string
	}{
		{
			Message: &v1.DispatchCheckRequest{
				ResourceRelation: tuple.RelationReference("document", "view"),
				ResourceIds:      []string{"foo", "bar"},
				Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
				Metadata: &v1.ResolverMeta{
					AtRevision: "1234",
				},
			},
			ExpectedHash: "24daf7c03e814283",
		},
		{
			Message: &v1.DispatchCheckRequest{
				ResourceRelation: tuple.RelationReference("document", "view"),
				ResourceIds:      []string{"foo", "bar"},
				Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
				Metadata: &v1.ResolverMeta{
					AtRevision: "12345",
				},
			},
			ExpectedHash: "0c14e2b0c5ce127b",
		},
		{
			Message: &v1.DispatchCheckRequest{
				ResourceRelation: tuple.RelationReference("document", "view"),
				ResourceIds:      []string{"foo", "bar", "baz"},
				Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
				Metadata: &v1.ResolverMeta{
					AtRevision: "1234",
				},
			},
			ExpectedHash: "091c0a405116e651",
		},
	} {
		tt := tt
		t.Run("", func(t *testing.T) {
			key, err := hashForDispatchCheck(tt.Message)
			require.NoError(t, err)
			require.Equal(t, tt.ExpectedHash, key)
		})
	}
}

func TestSingleFlightDispatcher(t *testing.T) {
	var called atomic.Uint64
	f := func() {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
	}
	disp := Dispatcher{delegate: mockDispatcher{f: f}}

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RelationReference("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
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
	disp := Dispatcher{delegate: mockDispatcher{f: f}}

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RelationReference("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
		},
	}

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
