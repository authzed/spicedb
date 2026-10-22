package remote

import (
	"math"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// latencyGateRefresh is the maximum age of the cached latency gate.
const latencyGateRefresh = time.Second

// latencySubBucketBits is the number of bits that select a sub-bucket in one power of two of microseconds.
// 3 bits give 8 sub-buckets for each power of two. A bucket is at most 1/8 of its lower bound wide.
const latencySubBucketBits = 3

// latencyBucketCount is the number of latency buckets in one window.
// The buckets cover 0µs to 2^27µs (about 134s). The last bucket also holds all larger latencies.
const latencyBucketCount = (27 - latencySubBucketBits + 1) << latencySubBucketBits

// ownerLatencies records the primary dispatch latency to each ring owner.
// Windows of fixed length start at multiples of window since the Unix epoch.
// Each owner keeps a latency histogram for the current window and a latency histogram for the previous window.
type ownerLatencies struct {
	// window is the length of one latency window.
	window time.Duration

	// now returns the current time. Tests replace it with a fake clock.
	now func() time.Time

	// digests maps each ring owner to its latency windows.
	digests *xsync.Map[string, *ownerDigest]

	// cached is the last gate that gate built. It is nil until the first build.
	cached atomic.Pointer[cachedGate]

	// rebuilding is true while one caller rebuilds the cached gate.
	rebuilding atomic.Bool

	// firstBuilt closes when the first gate is in cached.
	firstBuilt chan struct{}
	firstOnce  sync.Once
}

// cachedGate is a latency gate and its build time.
type cachedGate struct {
	gate    latencyGate
	builtAt time.Time
}

// latencyWindow counts the latency samples of one window in log-scale buckets.
type latencyWindow struct {
	// epoch is the number of the window that the buckets count.
	epoch atomic.Int64

	// buckets holds the number of samples in each latency bucket.
	buckets [latencyBucketCount]atomic.Uint64
}

// ownerDigest holds the latency windows of one ring owner.
// windows[e&1] is the window for epoch e. One ownerDigest uses about 3.2 KB.
type ownerDigest struct {
	windows [2]latencyWindow
}

// newOwnerLatencies returns an empty ownerLatencies with windows of the given length.
// The window must be positive, because epoch divides by it. A window that is not positive is a bug,
// and newOwnerLatencies then uses spreadWindow.
func newOwnerLatencies(window time.Duration, now func() time.Time) *ownerLatencies {
	if window <= 0 {
		log.Warn().Err(spiceerrors.MustBugf("owner latency window must be positive, got %v", window)).
			Dur("window", spreadWindow).Msg("owner latency window is not positive, so the default window is used")
		window = spreadWindow
	}

	return &ownerLatencies{
		window:     window,
		now:        now,
		digests:    xsync.NewMap[string, *ownerDigest](),
		firstBuilt: make(chan struct{}),
	}
}

// newOwnerDigest returns an empty ownerDigest whose window for epoch is current.
// snapshot does not remove the digest before epoch+2, even if record has not added a sample yet.
func newOwnerDigest(epoch int64) *ownerDigest {
	d := &ownerDigest{}
	d.windows[epoch&1].epoch.Store(epoch)
	return d
}

// latencyBucket returns the index of the bucket for latency.
// Latencies below 8µs have one bucket for each microsecond. Larger latencies have 8 buckets for each power of two.
func latencyBucket(latency time.Duration) int {
	micros := uint64(max(latency.Microseconds(), 0))
	shift := max(bits.Len64(micros)-latencySubBucketBits-1, 0)
	// nolint:gosec
	// G115: micros>>shift is less than 16, so the conversion is in range.
	return min(shift<<latencySubBucketBits+int(micros>>shift), latencyBucketCount-1)
}

// latencyBucketMillis returns the midpoint of bucket i in milliseconds.
func latencyBucketMillis(i int) float64 {
	shift := max(i>>latencySubBucketBits-1, 0)
	// nolint:gosec
	// G115: i-shift<<latencySubBucketBits is from 0 to 15, so the conversion is in range.
	lower := uint64(i-shift<<latencySubBucketBits) << shift
	width := uint64(1) << shift
	return (float64(lower) + float64(width)/2) / float64(time.Millisecond/time.Microsecond)
}

// latencyQuantile returns the quantile q of the bucket counts in milliseconds.
// total is the sum of the counts and must be positive.
func latencyQuantile(counts *[latencyBucketCount]uint64, total uint64, q float64) float64 {
	rank := max(uint64(math.Ceil(q*float64(total))), 1)
	var seen uint64
	for i, c := range counts {
		seen += c
		if seen >= rank {
			return latencyBucketMillis(i)
		}
	}
	return latencyBucketMillis(latencyBucketCount - 1)
}

// add counts one sample in bucket b of the window for epoch.
// The first caller in a new epoch resets the window. A sample that races the reset can be lost,
// or it can be counted in the reset window. A sample for an epoch older than the window is dropped.
func (d *ownerDigest) add(epoch int64, b int) {
	w := &d.windows[epoch&1]
	for {
		old := w.epoch.Load()
		if old == epoch {
			break
		}

		if old > epoch {
			return
		}

		if w.epoch.CompareAndSwap(old, epoch) {
			for i := range w.buckets {
				w.buckets[i].Store(0)
			}
			break
		}
	}
	w.buckets[b].Add(1)
}

// load returns a copy of the bucket counts and their sum if the window is for epoch.
// Otherwise it returns zero counts.
func (w *latencyWindow) load(epoch int64) (counts [latencyBucketCount]uint64, total uint64) {
	if w.epoch.Load() != epoch {
		return counts, 0
	}

	for i := range w.buckets {
		counts[i] = w.buckets[i].Load()
		total += counts[i]
	}
	return counts, total
}

func (o *ownerLatencies) epoch() int64 {
	return o.now().UnixNano() / int64(o.window)
}

// record adds one latency sample for owner. It does not lock and does not allocate after the owner has a digest.
// The caller gives the first ring member for the routing key as owner.
// That member receives the RPC only when the hashring spread is 1 (--dispatch-hashring-spread=1).
func (o *ownerLatencies) record(owner string, latency time.Duration) {
	epoch := o.epoch()
	d, ok := o.digests.Load(owner)
	if !ok {
		d, _ = o.digests.LoadOrStore(owner, newOwnerDigest(epoch))
	}
	d.add(epoch, latencyBucket(latency))
}

// gate returns the cached latency gate.
// It rebuilds the gate with snapshot if the gate is older than latencyGateRefresh.
// While one caller rebuilds, other callers use the old gate.
// Before the first gate exists, other callers wait for the first build.
func (o *ownerLatencies) gate() *latencyGate {
	now := o.now()
	cached := o.cached.Load()
	if cached != nil && now.Sub(cached.builtAt) < latencyGateRefresh {
		return &cached.gate
	}

	// The gate is old or missing. Only one caller rebuilds and stores it.
	if o.rebuilding.CompareAndSwap(false, true) {
		defer o.rebuilding.Store(false)
		rebuilt := &cachedGate{gate: o.snapshot(), builtAt: now}
		o.cached.Store(rebuilt)
		o.firstOnce.Do(func() { close(o.firstBuilt) })
		return &rebuilt.gate
	}

	// Another caller rebuilds the gate. Use the old gate.
	if cached != nil {
		return &cached.gate
	}

	// There is no gate yet. Wait for the first build and use its gate.
	<-o.firstBuilt
	return &o.cached.Load().gate
}

// latencyGate is a snapshot of the p90 latency of the owners that have enough samples.
type latencyGate struct {
	// p90 is the p90 latency of each owner in milliseconds.
	p90 map[string]float64

	// median is the lower median of the p90 values in milliseconds.
	median float64
}

// snapshot returns the p90 latency of each owner with at least minimumDigestCount samples.
// For each owner, it uses the current window if that window has enough samples, and the previous window if not.
// It removes owners that have no current or previous window.
// Only gate calls it on the dispatch path, at most once per latencyGateRefresh.
func (o *ownerLatencies) snapshot() latencyGate {
	epoch := o.epoch()
	gate := latencyGate{p90: map[string]float64{}}
	o.digests.Range(func(owner string, d *ownerDigest) bool {
		current := &d.windows[epoch&1]
		previous := &d.windows[(epoch-1)&1]
		if current.epoch.Load() < epoch && previous.epoch.Load() < epoch-1 {
			// The owner is idle. Delete d only if the map still holds d, so a new digest for owner stays.
			// A record that loaded d before the delete adds its sample to d, and that sample is lost.
			o.digests.Compute(owner, func(old *ownerDigest, loaded bool) (*ownerDigest, xsync.ComputeOp) {
				if loaded && old == d {
					return old, xsync.DeleteOp
				}
				return old, xsync.CancelOp
			})
			return true
		}

		counts, total := current.load(epoch)
		if total < minimumDigestCount {
			counts, total = previous.load(epoch - 1)
		}

		if total >= minimumDigestCount {
			gate.p90[owner] = latencyQuantile(&counts, total, defaultHedgerQuantile)
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
