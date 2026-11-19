package readiness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"resenje.org/singleflight"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	// healthCheckPrefix is the gRPC method prefix for health checks.
	// We bypass readiness checks for health endpoints so Kubernetes probes work.
	healthCheckPrefix = "/grpc.health.v1.Health/"

	// notReadyCacheTTL is how long to cache a not-migrated state before checking again.
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

// Gate blocks gRPC requests until the datastore has been migrated.
//
// Once the datastore reports ready, the gate stays open for the lifetime of
// the process, mirroring the health checker. This avoids a datastore call per
// request and ensures that running servers keep serving when a newer version
// of SpiceDB migrates the datastore ahead of them during a rolling upgrade.
type Gate struct {
	checker ReadinessChecker

	// open is set once the datastore has reported ready.
	open atomic.Bool

	// singleflight prevents thundering herd when the not-ready cache expires
	sfGroup singleflight.Group[string, string]

	mu            sync.RWMutex
	cachedMessage string    // GUARDED_BY(mu)
	cacheTime     time.Time // GUARDED_BY(mu)
}

// NewGate creates a new readiness gate with the given checker.
// If checker is nil, the gate will pass through all requests without checking.
func NewGate(checker ReadinessChecker) *Gate {
	return &Gate{checker: checker}
}

// isMigrationIssue returns true if the not-ready message indicates
// the database schema hasn't been migrated. Other not-ready reasons
// (like connection pool warmup) are transient and shouldn't block requests.
func isMigrationIssue(msg string) bool {
	return strings.Contains(msg, "not migrated")
}

// notMigratedMessage returns an empty string if requests may proceed, or the
// datastore's not-migrated message if they should be rejected.
func (g *Gate) notMigratedMessage(ctx context.Context) string {
	if g.checker == nil || g.open.Load() {
		return ""
	}

	if msg, ok := g.cachedNotMigrated(); ok {
		return msg
	}

	msg, _, _ := g.sfGroup.Do(ctx, "readiness", func(_ context.Context) (string, error) {
		if msg, ok := g.cachedNotMigrated(); ok {
			return msg, nil
		}

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
			return "", nil
		}

		if state.IsReady {
			g.open.Store(true)
			return "", nil
		}

		// Only block requests for migration-related issues.
		// Transient states (connection pool warmup, etc.) should not block.
		if !isMigrationIssue(state.Message) {
			return "", nil
		}

		g.mu.Lock()
		g.cachedMessage = state.Message
		g.cacheTime = time.Now()
		g.mu.Unlock()

		return state.Message, nil
	})

	return msg
}

func (g *Gate) cachedNotMigrated() (string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.cachedMessage != "" && time.Since(g.cacheTime) < notReadyCacheTTL {
		return g.cachedMessage, true
	}
	return "", false
}

func notMigratedError(msg string) error {
	return spiceerrors.WithCodeAndReason(
		errors.New(msg),
		codes.FailedPrecondition,
		v1.ErrorReason_ERROR_REASON_DATASTORE_NOT_MIGRATED,
	)
}

// UnaryServerInterceptor returns a gRPC unary interceptor that blocks
// requests until the datastore is migrated.
func (g *Gate) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Bypass health checks so Kubernetes probes work
		if strings.HasPrefix(info.FullMethod, healthCheckPrefix) {
			return handler(ctx, req)
		}

		if msg := g.notMigratedMessage(ctx); msg != "" {
			return nil, notMigratedError(msg)
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that blocks
// streams until the datastore is migrated.
func (g *Gate) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Bypass health checks so Kubernetes probes work
		if strings.HasPrefix(info.FullMethod, healthCheckPrefix) {
			return handler(srv, ss)
		}

		if msg := g.notMigratedMessage(ss.Context()); msg != "" {
			return notMigratedError(msg)
		}

		return handler(srv, ss)
	}
}
