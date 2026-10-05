// Package querysignalstest supplies deterministic fixtures for signal properties.
package querysignalstest

import (
	"sync"
	"time"
)

// Clock is a synchronized fake clock for deterministic signal tests.
type Clock struct {
	mu  sync.Mutex
	now time.Time // GUARDED_BY(mu)
}

// NewClock creates a fake clock starting at a fixed UTC instant.
func NewClock() *Clock { return &Clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

// Now returns the fake clock's current time.
func (c *Clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

// Advance moves the fake clock forward by the supplied duration.
func (c *Clock) Advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }
