package servicespecific

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestEligibleRequest(t *testing.T) {
	for _, tt := range []struct {
		Message      proto.Message
		Eligible     bool
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
			Eligible:     true,
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
			Eligible:     true,
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
			Eligible:     true,
			ExpectedHash: "091c0a405116e651",
		},
		{
			Message:      &v1.DispatchExpandRequest{},
			Eligible:     false,
			ExpectedHash: "",
		},
	} {
		tt := tt
		t.Run("", func(t *testing.T) {
			key, eligible, err := eligibleRequest(tt.Message)
			require.NoError(t, err)
			require.Equal(t, tt.Eligible, eligible)
			if tt.Eligible {
				require.Equal(t, tt.ExpectedHash, key)
			}
		})
	}
}

func TestSingleFlightMiddleware(t *testing.T) {
	mw := UnaryServerInterceptor()
	var called atomic.Uint64
	handler := func(ctx context.Context, req any) (any, error) {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
		return nil, nil
	}

	req := &v1.DispatchCheckRequest{
		ResourceRelation: tuple.RelationReference("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(5)
	go func() {
		_, _ = mw(context.Background(), req, nil, handler)
		wg.Done()
	}()
	go func() {
		_, _ = mw(context.Background(), req, nil, handler)
		wg.Done()
	}()
	go func() {
		_, _ = mw(context.Background(), req, nil, handler)

		wg.Done()
	}()
	go func() {
		_, _ = mw(context.Background(), &v1.DispatchExpandRequest{}, nil, handler)
		wg.Done()
	}()
	go func() {
		_, _ = mw(context.Background(), &v1.DispatchCheckRequest{
			ResourceRelation: tuple.RelationReference("document", "view"),
			ResourceIds:      []string{"foo", "baz"},
			Subject:          tuple.ObjectAndRelation("user", "tom", "..."),
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
			},
		}, nil, handler)
		wg.Done()
	}()

	wg.Wait()

	require.Equal(t, uint64(3), called.Load())
}

func TestSingleFlightMiddlewareCancelation(t *testing.T) {
	mw := UnaryServerInterceptor()
	var called atomic.Uint64
	run := make(chan struct{}, 1)
	handler := func(ctx context.Context, req any) (any, error) {
		time.Sleep(100 * time.Millisecond)
		called.Add(1)
		run <- struct{}{}
		return nil, nil
	}

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
		_, err := mw(ctx, req, nil, handler)
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := mw(ctx, req, nil, handler)
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err := mw(ctx, req, nil, handler)
		wg.Done()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()

	wg.Wait()
	<-run
	require.Equal(t, uint64(1), called.Load())
}
