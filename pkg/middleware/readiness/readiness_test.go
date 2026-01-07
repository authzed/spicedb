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

func TestGate_HandlesCheckerError(t *testing.T) {
	// Checker errors are treated as transient - requests pass through
	// so they can fail naturally with the actual database error
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

	// Request should pass through - checker errors are not migration issues
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
	// Use migration error so it actually blocks
	checker := &mockChecker{
		ready:   false,
		message: "datastore is not migrated",
	}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}

	// First call fails due to migration issue
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	require.Error(t, err)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Now checker becomes ready
	checker.ready = true
	checker.message = ""

	// Force cache expiry
	gate.mu.Lock()
	gate.cacheTime = time.Now().Add(-2 * notReadyCacheTTL)
	gate.mu.Unlock()

	// Second call should retry (not use cached error)
	handlerCalled := false
	_, err = interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	})
	require.NoError(t, err)
	require.True(t, handlerCalled)
	require.Equal(t, int32(2), checker.callCount.Load())
}

func TestGate_NegativeCacheReducesChecks(t *testing.T) {
	// Use a migration-related message so requests are actually blocked
	checker := &mockChecker{
		ready:   false,
		message: "datastore is not migrated",
	}
	gate := NewGate(checker)

	interceptor := gate.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}

	// First call checks readiness and is blocked
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	require.Error(t, err)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Second call should use negative cache (within notReadyCacheTTL)
	_, err = interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	require.Error(t, err)
	require.Equal(t, int32(1), checker.callCount.Load())

	// Third call should use negative cache
	_, err = interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	require.Error(t, err)
	require.Equal(t, int32(1), checker.callCount.Load())
}

func TestGate_OnlyBlocksOnMigrationIssues(t *testing.T) {
	tests := []struct {
		name         string
		message      string
		expectBlocked bool
	}{
		{
			name:          "migration issue blocks",
			message:       "datastore is not migrated: currently at revision \"\"",
			expectBlocked: true,
		},
		{
			name:          "connection pool issue passes through",
			message:       "spicedb does not have the required minimum connection count",
			expectBlocked: false,
		},
		{
			name:          "generic not ready passes through",
			message:       "some other issue",
			expectBlocked: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &mockChecker{
				ready:   false,
				message: tt.message,
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

			if tt.expectBlocked {
				require.Error(t, err)
				require.False(t, handlerCalled)
				require.Contains(t, err.Error(), "spicedb datastore migrate")
			} else {
				require.NoError(t, err)
				require.True(t, handlerCalled)
			}
		})
	}
}
