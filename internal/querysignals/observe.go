// Package querysignals bridges planner execution to typed signal providers.
package querysignals

import (
	"sync"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type (

	// CollectorOptions bounds the number of buffered execution observations.
	CollectorOptions struct {
		// MaxObservations limits terminal events buffered before request-exit publication.
		MaxObservations int
	}

	// Collector buffers terminal execution events and publishes them once.
	Collector struct {
		mu      sync.Mutex
		view    *signals.View
		max     int
		events  []query.ExecutionObservation // GUARDED_BY(mu)
		flushed bool
		dropped uint64
	}
)

// NewCollector creates a bounded request-local execution collector.
func NewCollector(v *signals.View, opts CollectorOptions) *Collector {
	if opts.MaxObservations <= 0 {
		opts.MaxObservations = 4096
	}

	return &Collector{view: v, max: opts.MaxObservations}
}

// ObserveExecution buffers a terminal event unless the collector is flushed or full.
func (c *Collector) ObserveExecution(e query.ExecutionObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.flushed || len(c.events) >= c.max || e.Key.Identity == "" {
		c.dropped++
		return
	}

	c.events = append(c.events, e)
}

// Dropped returns the number of events rejected during buffering or publication.
func (c *Collector) Dropped() uint64 { c.mu.Lock(); defer c.mu.Unlock(); return c.dropped }

// Flush publishes buffered terminal events once; repeated calls have no effect.
func (c *Collector) Flush() {
	c.mu.Lock()

	if c.flushed {
		c.mu.Unlock()
		return
	}

	c.flushed = true
	events := c.events
	c.events = nil
	c.mu.Unlock()

	for _, e := range events {
		if e.Key.Operation == signal.Check {
			if !c.view.Enabled(branchoutcome.Name) {
				continue
			}

			if e.Status != signal.Complete {
				c.observeOutcome(
					e,
					branchoutcome.Observation{Status: e.Status, Started: e.Started, UnresolvedAttemptUpperBound: e.BatchSize},
				)
				continue
			}

			for _, v := range []struct {
				n       uint64
				outcome branchoutcome.Outcome
			}{{e.Denied, branchoutcome.Deny}, {e.Granted, branchoutcome.Grant}, {e.Conditional, branchoutcome.Conditional}} {
				for range v.n {
					c.observeOutcome(e, branchoutcome.Observation{Status: signal.Complete, Outcome: v.outcome, Started: e.Started})
				}
			}
		} else if c.view.Enabled(resultsize.Name) {
			if err := signal.Observe(
				c.view,
				resultsize.Input,
				e.Key,
				resultsize.Observation{Count: e.Count, Status: e.Status, Started: e.Started},
			); err != nil {
				c.drop()
			}
		}
	}
}

func (c *Collector) observeOutcome(e query.ExecutionObservation, o branchoutcome.Observation) {
	if err := signal.Observe(c.view, branchoutcome.Input, e.Key, o); err != nil {
		c.drop()
	}
}

func (c *Collector) drop() { c.mu.Lock(); c.dropped++; c.mu.Unlock() }
