package readiness

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/datastore"
)

type mockChecker struct {
	ready     bool
	message   string
	err       error
	callCount atomic.Int32
}

func (m *mockChecker) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	m.callCount.Add(1)
	if m.err != nil {
		return datastore.ReadyState{}, m.err
	}
	return datastore.ReadyState{
		IsReady: m.ready,
		Message: m.message,
	}, nil
}

func TestGate_BlocksWhenNotReady(t *testing.T) {
	checker := &mockChecker{
		ready:   false,
		message: "datastore is not migrated",
	}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/authzed.api.v1.PermissionsService/CheckPermission",
	}, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called when not ready")
		return nil, nil
	})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Contains(t, st.Message(), "not migrated")
	require.Contains(t, st.Message(), "spicedb datastore migrate")
}

func TestGate_AllowsWhenReady(t *testing.T) {
	checker := &mockChecker{ready: true}
	gate := NewGate(checker)

	handlerCalled := false
	interceptor := gate.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/authzed.api.v1.PermissionsService/CheckPermission",
	}, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "response", nil
	})

	require.NoError(t, err)
	require.True(t, handlerCalled)
}

func TestGate_BypassesHealthCheck(t *testing.T) {
	checker := &mockChecker{
		ready:   false,
		message: "not ready",
	}
	gate := NewGate(checker)

	handlerCalled := false
	interceptor := gate.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/grpc.health.v1.Health/Check",
	}, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})

	require.NoError(t, err)
	require.True(t, handlerCalled)
	// Checker should not be called for health checks
	require.Equal(t, int32(0), checker.callCount.Load())
}

func TestGate_BypassesHealthWatch(t *testing.T) {
	checker := &mockChecker{
		ready:   false,
		message: "not ready",
	}
	gate := NewGate(checker)

	handlerCalled := false
	interceptor := gate.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/grpc.health.v1.Health/Watch",
	}, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})

	require.NoError(t, err)
	require.True(t, handlerCalled)
}

func TestGate_CachesReadyState(t *testing.T) {
	checker := &mockChecker{ready: true}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	// First call should check readiness
	_, _ = interceptor(context.Background(), nil, info, handler)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Second call should use cache
	_, _ = interceptor(context.Background(), nil, info, handler)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Third call should use cache
	_, _ = interceptor(context.Background(), nil, info, handler)
	require.Equal(t, int32(1), checker.callCount.Load())
}

func TestGate_CacheExpires(t *testing.T) {
	checker := &mockChecker{ready: true}
	gate := NewGate(checker)

	// Override cache time to test expiry
	gate.mu.Lock()
	gate.cachedReady = true
	gate.cacheTime = time.Now().Add(-2 * readyCacheTTL) // Expired
	gate.mu.Unlock()

	interceptor := gate.UnaryServerInterceptor()
	_, _ = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/test/Method",
	}, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})

	// Should have called checker because cache expired
	require.Equal(t, int32(1), checker.callCount.Load())
}

func TestGate_SingleflightPreventsThunderingHerd(t *testing.T) {
	// Slow checker that takes 100ms
	checker := &mockChecker{ready: true}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	// Launch 10 concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = interceptor(context.Background(), nil, info, handler)
		}()
	}
	wg.Wait()

	// Singleflight should have deduplicated the calls
	// We might get 1 or 2 calls depending on timing, but definitely not 10
	require.LessOrEqual(t, checker.callCount.Load(), int32(2))
}

func TestGate_PassesThroughOnCheckerError(t *testing.T) {
	checker := &mockChecker{
		err: errors.New("connection refused"),
	}
	gate := NewGate(checker)

	handlerCalled := false
	interceptor := gate.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/test/Method",
	}, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})

	// Errors during readiness check should allow requests through.
	// If the datastore is truly unavailable, requests will fail at the
	// datastore layer with appropriate errors.
	require.NoError(t, err)
	require.True(t, handlerCalled)
}

func TestGate_StreamInterceptorBlocksWhenNotReady(t *testing.T) {
	checker := &mockChecker{
		ready:   false,
		message: "not migrated",
	}
	gate := NewGate(checker)

	interceptor := gate.StreamServerInterceptor()
	err := interceptor(nil, &mockServerStream{}, &grpc.StreamServerInfo{
		FullMethod: "/authzed.api.v1.WatchService/Watch",
	}, func(srv any, stream grpc.ServerStream) error {
		t.Fatal("handler should not be called when not ready")
		return nil
	})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestGate_StreamInterceptorAllowsWhenReady(t *testing.T) {
	checker := &mockChecker{ready: true}
	gate := NewGate(checker)

	handlerCalled := false
	interceptor := gate.StreamServerInterceptor()
	err := interceptor(nil, &mockServerStream{}, &grpc.StreamServerInfo{
		FullMethod: "/authzed.api.v1.WatchService/Watch",
	}, func(srv any, stream grpc.ServerStream) error {
		handlerCalled = true
		return nil
	})

	require.NoError(t, err)
	require.True(t, handlerCalled)
}

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func TestGate_NilCheckerPassesThrough(t *testing.T) {
	gate := NewGate(nil)

	handlerCalled := false
	interceptor := gate.UnaryServerInterceptor()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/authzed.api.v1.PermissionsService/CheckPermission",
	}, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "response", nil
	})

	require.NoError(t, err)
	require.True(t, handlerCalled)
}

func TestGate_ErrorDoesNotCache(t *testing.T) {
	checker := &mockChecker{err: errors.New("temporary connection error")}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}

	// First call: checker errors but request passes through
	handlerCalled := false
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})
	require.NoError(t, err)
	require.True(t, handlerCalled)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Second call should retry checker (errors are not cached)
	handlerCalled = false
	_, err = interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})
	require.NoError(t, err)
	require.True(t, handlerCalled)
	require.Equal(t, int32(2), checker.callCount.Load())
}

func TestGate_NegativeCacheReducesChecks(t *testing.T) {
	checker := &mockChecker{
		ready:   false,
		message: "datastore is not migrated",
	}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	// First call checks readiness
	_, _ = interceptor(context.Background(), nil, info, handler)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Second call should use negative cache (within notReadyCacheTTL)
	_, _ = interceptor(context.Background(), nil, info, handler)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Third call should use negative cache
	_, _ = interceptor(context.Background(), nil, info, handler)
	require.Equal(t, int32(1), checker.callCount.Load())
}

func TestGate_ErrorMessageContextAware(t *testing.T) {
	t.Run("migration issue blocks with actionable message", func(t *testing.T) {
		checker := &mockChecker{
			ready:   false,
			message: "datastore is not migrated: currently at revision \"\"",
		}
		gate := NewGate(checker)

		interceptor := gate.UnaryServerInterceptor()
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
			FullMethod: "/test/Method",
		}, func(ctx context.Context, req any) (any, error) {
			t.Fatal("handler should not be called for migration issues")
			return nil, nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "spicedb datastore migrate")
		require.Contains(t, err.Error(), "not migrated")
	})

	t.Run("connection pool issue passes through", func(t *testing.T) {
		checker := &mockChecker{
			ready:   false,
			message: "spicedb does not have the required minimum connection count",
		}
		gate := NewGate(checker)

		handlerCalled := false
		interceptor := gate.UnaryServerInterceptor()
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
			FullMethod: "/test/Method",
		}, func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			return "ok", nil
		})

		require.NoError(t, err)
		require.True(t, handlerCalled)
	})

	t.Run("generic not ready passes through", func(t *testing.T) {
		checker := &mockChecker{
			ready:   false,
			message: "some other issue",
		}
		gate := NewGate(checker)

		handlerCalled := false
		interceptor := gate.UnaryServerInterceptor()
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
			FullMethod: "/test/Method",
		}, func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			return "ok", nil
		})

		require.NoError(t, err)
		require.True(t, handlerCalled)
	})
}
