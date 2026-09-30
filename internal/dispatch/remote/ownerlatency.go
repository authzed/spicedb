package remote

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caio/go-tdigest/v4"
	"github.com/puzpuzpuz/xsync/v4"
)

// ownerLatencyCompression is the t-digest compression for the latency of one ring owner.
const ownerLatencyCompression = float64(100)

// latencyGateRefresh is the maximum age of the cached latency gate.
const latencyGateRefresh = time.Second

// ownerLatencies records the primary dispatch latency to each ring owner.
// Windows of fixed length start at multiples of window since the Unix epoch.
// Each owner keeps a digest for the current window and a digest for the previous window.
type ownerLatencies struct {
	window  time.Duration
	now     func() time.Time
	digests *xsync.Map[string, *ownerDigest]

	// cached is the last gate that gate built.
	cached atomic.Pointer[cachedGate]
	// rebuilding is true while one caller rebuilds the cached gate.
	rebuilding atomic.Bool
}

// cachedGate is a latency gate and the time that it was built.
type cachedGate struct {
	gate    latencyGate
	builtAt time.Time
}

// ownerDigest holds the latency digests of one ring owner.
// mu guards all other fields.
type ownerDigest struct {
	mu sync.Mutex
	// epoch is the number of the window of current.
	epoch    int64            // GUARDED_BY(mu)
	current  *tdigest.TDigest // GUARDED_BY(mu)
	previous *tdigest.TDigest // GUARDED_BY(mu)
	// removed is true after snapshot removes the digest from the map.
	removed bool // GUARDED_BY(mu)
}

func newOwnerLatencies(window time.Duration, now func() time.Time) *ownerLatencies {
	return &ownerLatencies{window: window, now: now, digests: xsync.NewMap[string, *ownerDigest]()}
}

func (o *ownerLatencies) epoch() int64 {
	return o.now().UnixNano() / int64(o.window)
}

// rotate moves d to the given window.
// The caller must hold d.mu.
func (d *ownerDigest) rotate(epoch int64) {
	switch {
	case epoch <= d.epoch:
		return
	case epoch == d.epoch+1:
		d.previous.Reset()
		d.current, d.previous = d.previous, d.current
	default:
		d.current.Reset()
		d.previous.Reset()
	}
	d.epoch = epoch
}

// record adds one latency sample for owner.
// The caller gives the first ring member for the routing key as owner.
// That member receives the RPC only when the hashring spread is 1 (--dispatch-hashring-spread=1).
func (o *ownerLatencies) record(owner string, latency time.Duration) {
	epoch := o.epoch()
	for {
		d, _ := o.digests.LoadOrCompute(owner, func() (*ownerDigest, bool) {
			current, err := tdigest.New(tdigest.Compression(ownerLatencyCompression))
			if err != nil {
				return nil, true
			}
			previous, err := tdigest.New(tdigest.Compression(ownerLatencyCompression))
			if err != nil {
				return nil, true
			}
			return &ownerDigest{epoch: epoch, current: current, previous: previous}, false
		})
		if d == nil {
			return
		}
		d.mu.Lock()
		if d.removed {
			// snapshot is removing this digest. The next load gets a new digest.
			d.mu.Unlock()
			continue
		}
		d.rotate(epoch)
		// The error is only for invalid values, and a duration is always valid.
		_ = d.current.Add(float64(latency) / float64(time.Millisecond))
		d.mu.Unlock()
		return
	}
}

// gate returns the cached latency gate.
// It rebuilds the gate with snapshot if the gate is older than latencyGateRefresh.
// While one caller rebuilds, other callers use the previous gate.
func (o *ownerLatencies) gate() *latencyGate {
	now := o.now()
	cached := o.cached.Load()
	if cached != nil && now.Sub(cached.builtAt) < latencyGateRefresh {
		return &cached.gate
	}
	if !o.rebuilding.CompareAndSwap(false, true) {
		if cached != nil {
			return &cached.gate
		}
		gate := o.snapshot()
		return &gate
	}
	defer o.rebuilding.Store(false)
	rebuilt := &cachedGate{gate: o.snapshot(), builtAt: now}
	o.cached.Store(rebuilt)
	return &rebuilt.gate
}

// latencyGate is a snapshot of the p90 latency of the owners that have enough samples.
type latencyGate struct {
	// p90 is in milliseconds.
	p90    map[string]float64
	median float64
}

// snapshot returns the p90 latency of each owner with at least minimumDigestCount samples.
// For each owner, it uses the current window if that window has enough samples, and the previous window if not.
// It removes owners that have no samples in either window.
// Only gate calls it on the dispatch path, at most once per latencyGateRefresh.
func (o *ownerLatencies) snapshot() latencyGate {
	epoch := o.epoch()
	gate := latencyGate{p90: map[string]float64{}}
	o.digests.Range(func(owner string, d *ownerDigest) bool {
		d.mu.Lock()
		d.rotate(epoch)
		switch {
		case d.current.Count() >= minimumDigestCount:
			gate.p90[owner] = d.current.Quantile(defaultHedgerQuantile)
		case d.previous.Count() >= minimumDigestCount:
			gate.p90[owner] = d.previous.Quantile(defaultHedgerQuantile)
		}
		idle := d.current.Count() == 0 && d.previous.Count() == 0
		if idle {
			d.removed = true
		}
		d.mu.Unlock()
		if idle {
			o.digests.Compute(owner, func(old *ownerDigest, loaded bool) (*ownerDigest, xsync.ComputeOp) {
				if loaded && old == d {
					return old, xsync.DeleteOp
				}
				return old, xsync.CancelOp
			})
		}
		return true
	})

	if len(gate.p90) > 0 {
		values := make([]float64, 0, len(gate.p90))
		for _, v := range gate.p90 {
			values = append(values, v)
		}
		slices.Sort(values)
		// The lower median lets one slow owner of two pass a factor below 2.
		gate.median = values[(len(values)-1)/2]
	}
	return gate
}

// allows reports whether the latency of owner permits a spread.
// With fewer than 2 owners that have enough samples, it always permits a spread.
// Otherwise, owner must have enough samples and a p90 of at least factor times the median p90.
func (g latencyGate) allows(owner string, factor float64) bool {
	if len(g.p90) < 2 {
		return true
	}
	p90, ok := g.p90[owner]
	if !ok {
		return false
	}
	return p90 >= factor*g.median
}
