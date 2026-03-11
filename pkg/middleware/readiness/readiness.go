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

// isMigrationIssue returns true if the not-ready message indicates
// the database schema hasn't been migrated. Other not-ready reasons
// (like connection pool warmup) are transient and shouldn't block requests.
func isMigrationIssue(msg string) bool {
	return strings.Contains(msg, "not migrated") || strings.Contains(msg, "migration")
}

// isReady checks if the datastore is ready, using a cached value if available.
// Uses singleflight to prevent thundering herd on cache expiry.
// Only blocks requests for migration-related issues; transient states like
// connection pool warmup are allowed through.
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
			// On error checking readiness, allow requests through.
			// If the datastore is truly unavailable, requests will fail
			// with appropriate errors from the datastore layer.
			return readinessResult{ready: true, message: ""}, nil
		}

		// Only block requests for migration-related issues.
		// Transient states (connection pool warmup, etc.) should not block.
		ready := state.IsReady || !isMigrationIssue(state.Message)

		// Update cache
		g.mu.Lock()
		g.cachedReady = ready
		g.cachedMessage = state.Message
		g.cacheTime = time.Now()
		g.mu.Unlock()

		return readinessResult{ready: ready, message: state.Message}, nil
	})

	r := result.(readinessResult)
	return r.ready, r.message
}

// formatNotReadyError creates a user-friendly error message based on the readiness failure reason.
// TODO(authzed/api#159): Once ERROR_REASON_DATASTORE_NOT_MIGRATED is available in the API,
// use spiceerrors.WithCodeAndReason to include the structured error reason.
func formatNotReadyError(msg string) error {
	// Check if this is a migration-related issue
	if strings.Contains(msg, "not migrated") || strings.Contains(msg, "migration") {
		return status.Errorf(codes.FailedPrecondition,
			"SpiceDB datastore is not migrated. Please run 'spicedb datastore migrate'. Details: %s", msg)
	}
	// Generic not-ready message for other cases (connection issues, pool not ready, etc.)
	return status.Errorf(codes.FailedPrecondition,
		"SpiceDB datastore is not ready. Details: %s", msg)
}

// UnaryServerInterceptor returns a gRPC unary interceptor that blocks
// requests until the datastore is ready.
func (g *Gate) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Bypass health checks so Kubernetes probes work
		if strings.HasPrefix(info.FullMethod, healthCheckPrefix) {
			return handler(ctx, req)
		}

		ready, msg := g.isReady(ctx)
		if !ready {
			return nil, formatNotReadyError(msg)
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that blocks
// streams until the datastore is ready.
func (g *Gate) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Bypass health checks so Kubernetes probes work
		if strings.HasPrefix(info.FullMethod, healthCheckPrefix) {
			return handler(srv, ss)
		}

		ready, msg := g.isReady(ss.Context())
		if !ready {
			return formatNotReadyError(msg)
		}

		return handler(srv, ss)
	}
}
