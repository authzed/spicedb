// Package execscope tracks asynchronous work for measurement phase boundaries.
package execscope

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ccoveille/go-safecast/v2"
)

type contextKey struct{}

// Scope tracks workers registered before launch, including nested workers.
// A caller must retain a root registration until it has finished launching work.
type Scope struct {
	work      sync.WaitGroup
	setChecks atomic.Uint64
	fallbacks atomic.Uint64
	width     atomic.Uint32
	waits     atomic.Uint64
	waitNS    atomic.Uint64
}

// Statistics describes set scheduling policy observed within a measurement scope.
type Statistics struct {
	// QuiescenceWaits counts phase barriers, including waits with no outstanding work.
	QuiescenceWaits uint64
	// QuiescenceWaitNS is total phase-barrier time outside foreground Check latency.
	QuiescenceWaitNS uint64

	// SetChecks counts invocations with a fixed concurrency recommendation.
	SetChecks uint64
	// SerialFallbacks counts observer/frontier declines of parallel execution.
	SerialFallbacks uint64
	// MaxEffectiveWidth is the largest configured effective reducer width observed.
	// It measures policy, not active goroutines or simultaneous datastore queries.
	MaxEffectiveWidth uint16
}

// Statistics returns a concurrency-safe snapshot of observed policy counters.
func (s *Scope) Statistics() Statistics {
	return Statistics{
		SetChecks:         s.setChecks.Load(),
		SerialFallbacks:   s.fallbacks.Load(),
		MaxEffectiveWidth: safecast.MustConvert[uint16](s.width.Load()),
		QuiescenceWaits:   s.waits.Load(),
		QuiescenceWaitNS:  s.waitNS.Load(),
	}
}

// ResetStatistics starts a new measurement population after all work has quiesced.
func (s *Scope) ResetStatistics() {
	s.setChecks.Store(0)
	s.fallbacks.Store(0)
	s.width.Store(0)
	s.waits.Store(0)
	s.waitNS.Store(0)
}

// RecordConcurrency records a set policy only when measurement tracking is enabled.
func RecordConcurrency(ctx context.Context, requested, effective uint16, fallback bool) {
	scope, _ := ctx.Value(contextKey{}).(*Scope)
	if scope == nil || requested == 0 {
		return
	}

	scope.setChecks.Add(1)

	if fallback {
		scope.fallbacks.Add(1)
	}

	width := uint32(effective)

	for prior := scope.width.Load(); width > prior && !scope.width.CompareAndSwap(prior, width); prior = scope.width.Load() {
	}
}

// New creates an empty execution scope.
func New() *Scope { return &Scope{} }

// Context attaches the scope without changing cancellation or scheduling policy.
func (s *Scope) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey{}, s)
}

// Wait waits for registered work to finish outside the request latency boundary.
func (s *Scope) Wait() {
	started := time.Now()
	s.work.Wait()
	s.waits.Add(1)
	s.waitNS.Add(safecast.MustConvert[uint64](time.Since(started).Nanoseconds()))
}

func untracked() {}

// Begin registers work before launch and returns its completion function.
// Contexts without a scope incur no synchronization or scheduling change.
func Begin(ctx context.Context) func() {
	scope, _ := ctx.Value(contextKey{}).(*Scope)
	if scope == nil {
		return untracked
	}

	scope.work.Add(1)
	return scope.work.Done
}
