package readiness

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	// healthCheckPrefix is the gRPC method prefix for health checks.
	// We bypass readiness checks for health endpoints so Kubernetes probes work.
	healthCheckPrefix = "/grpc.health.v1.Health/"

	// readyCacheTTL is how long to cache a positive ready state.
	readyCacheTTL = 500 * time.Millisecond

	// notReadyCacheTTL is how long to cache a negative ready state.
	// Shorter than readyCacheTTL to allow faster recovery detection.
	notReadyCacheTTL = 100 * time.Millisecond

	// readinessCheckTimeout is the maximum time to wait for a readiness check.
	// We use a dedicated context with this timeout inside singleflight to ensure
	// consistent behavior regardless of which request's context triggered the check.
	readinessCheckTimeout = 5 * time.Second
)

// ReadinessChecker is the interface for checking datastore readiness.
type ReadinessChecker interface {
	ReadyState(ctx context.Context) (datastore.ReadyState, error)
}

// Gate blocks gRPC requests until the datastore is ready.
// It caches the ready state briefly to avoid overwhelming the datastore
// with readiness checks on every request.
type Gate struct {
	checker ReadinessChecker

	// singleflight prevents thundering herd when cache expires
	sfGroup singleflight.Group

	mu            sync.RWMutex
	cachedReady   bool      // GUARDED_BY(mu)
	cachedMessage string    // GUARDED_BY(mu)
	cacheTime     time.Time // GUARDED_BY(mu)
}

// NewGate creates a new readiness gate with the given checker.
// If checker is nil, the gate will pass through all requests without checking.
func NewGate(checker ReadinessChecker) *Gate {
	return &Gate{checker: checker}
}

// readinessResult holds the result of a readiness check for singleflight.
type readinessResult struct {
	ready   bool
	message string
}

// isReady checks if the datastore is ready, using a cached value if available.
// Uses singleflight to prevent thundering herd on cache expiry.
func (g *Gate) isReady(ctx context.Context) (bool, string) {
	// If no checker is configured, pass through
	if g.checker == nil {
		return true, ""
	}

	// Fast path: check cache with read lock
	g.mu.RLock()
	elapsed := time.Since(g.cacheTime)
	ttl := readyCacheTTL
	if !g.cachedReady {
		ttl = notReadyCacheTTL
	}
	if elapsed < ttl {
		ready, msg := g.cachedReady, g.cachedMessage
		g.mu.RUnlock()
		return ready, msg
	}
	g.mu.RUnlock()

	// Slow path: use singleflight to deduplicate concurrent checks
	result, _, _ := g.sfGroup.Do("readiness", func() (any, error) {
		// Double-check cache after acquiring singleflight
		g.mu.RLock()
		elapsed := time.Since(g.cacheTime)
		ttl := readyCacheTTL
		if !g.cachedReady {
			ttl = notReadyCacheTTL
		}
		if elapsed < ttl {
			ready, msg := g.cachedReady, g.cachedMessage
			g.mu.RUnlock()
			return readinessResult{ready: ready, message: msg}, nil
		}
		g.mu.RUnlock()

		// Use an independent context with timeout for the readiness check.
		// This ensures consistent behavior when multiple requests are coalesced
		// by singleflight - we don't want the check to fail because the first
		// request's context was cancelled.
		checkCtx, cancel := context.WithTimeout(context.Background(), readinessCheckTimeout)
		defer cancel()

		state, err := g.checker.ReadyState(checkCtx)
		if err != nil {
			// Don't cache errors - allow retry on next request
			return readinessResult{
				ready:   false,
				message: "failed to check datastore readiness: " + err.Error(),
			}, nil
		}

		// Update cache (both positive and negative states)
		g.mu.Lock()
		g.cachedReady = state.IsReady
		g.cachedMessage = state.Message
		g.cacheTime = time.Now()
		g.mu.Unlock()

		return readinessResult{ready: state.IsReady, message: state.Message}, nil
	})

	r := result.(readinessResult)
	return r.ready, r.message
}

// isMigrationIssue checks if the readiness failure is due to missing migrations.
// We only want to block requests for migration issues, not for transient issues
// like connection pool warmup.
func isMigrationIssue(msg string) bool {
	return strings.Contains(msg, "not migrated") || strings.Contains(msg, "migration")
}

// formatNotReadyError creates a user-friendly error message for migration issues.
// TODO: Use ERROR_REASON_DATASTORE_NOT_MIGRATED once authzed-go is updated (authzed/api#159 merged)
func formatNotReadyError(msg string) error {
	return status.Errorf(codes.FailedPrecondition,
		"SpiceDB datastore is not migrated. Please run 'spicedb datastore migrate'. Details: %s", msg)
}

// UnaryServerInterceptor returns a gRPC unary interceptor that blocks
// requests until the datastore migrations have been applied.
// Note: This only blocks on migration issues, not on transient issues like
// connection pool warmup.
func (g *Gate) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Bypass health checks so Kubernetes probes work
		if strings.HasPrefix(info.FullMethod, healthCheckPrefix) {
			return handler(ctx, req)
		}

		ready, msg := g.isReady(ctx)
		if !ready && isMigrationIssue(msg) {
			return nil, formatNotReadyError(msg)
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that blocks
// streams until the datastore migrations have been applied.
// Note: This only blocks on migration issues, not on transient issues like
// connection pool warmup.
func (g *Gate) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Bypass health checks so Kubernetes probes work
		if strings.HasPrefix(info.FullMethod, healthCheckPrefix) {
			return handler(srv, ss)
		}

		ready, msg := g.isReady(ss.Context())
		if !ready && isMigrationIssue(msg) {
			return formatNotReadyError(msg)
		}

		return handler(srv, ss)
	}
}
