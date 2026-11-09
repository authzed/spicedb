package remote

import (
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// counts returns the sample count of the current window for each owner.
func (o *ownerLatencies) counts() map[string]uint64 {
	out := map[string]uint64{}
	epoch := o.epoch()
	o.digests.Range(func(owner string, d *ownerDigest) bool {
		if c := d.windowCount(epoch); c > 0 {
			out[owner] = c
		}
		return true
	})
	return out
}

// windowCount returns the sample count of the window for epoch.
func (d *ownerDigest) windowCount(epoch int64) uint64 {
	_, total := d.windows[epoch&1].load(epoch)
	return total
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time // GUARDED_BY(mu)
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func addSamples(o *ownerLatencies, owner string, d time.Duration, count int) {
	for range count {
		o.record(owner, d)
	}
}

func TestLatencyGate(t *testing.T) {
	type samples struct {
		owner   string
		latency time.Duration
		count   int
	}
	type check struct {
		owner  string
		factor float64
		want   bool
		msg    string
	}
	for _, tc := range []struct {
		name       string
		samples    []samples
		wantMedian float64
		checks     []check
	}{
		{
			name: "elevated and normal owners",
			samples: []samples{
				{"slow", 10 * time.Millisecond, minimumDigestCount},
				{"a", time.Millisecond, minimumDigestCount},
				{"b", time.Millisecond, minimumDigestCount},
				{"few", time.Second, minimumDigestCount - 1},
			},
			// The median uses only owners with enough samples.
			wantMedian: 1,
			checks: []check{
				{"slow", 1.5, true, "an elevated owner allows spread"},
				{"a", 1.5, false, "a normal owner blocks spread"},
				{"few", 1.5, false, "an owner without enough samples blocks spread"},
				{"unknown", 1.5, false, "an unknown owner blocks spread"},
				{"slow", 10.5, false, "a factor above the ratio blocks spread"},
			},
		},
		{
			name: "sub-millisecond resolution",
			samples: []samples{
				{"slow", 600 * time.Microsecond, minimumDigestCount},
				{"a", 200 * time.Microsecond, minimumDigestCount},
				{"b", 200 * time.Microsecond, minimumDigestCount},
			},
			wantMedian: 0.2,
			checks: []check{
				{"slow", 1.5, true, "an elevated owner allows spread"},
				{"a", 1.5, false, "a normal owner blocks spread"},
			},
		},
		{
			name: "two owners",
			samples: []samples{
				{"slow", 2 * time.Millisecond, minimumDigestCount},
				{"fast", time.Millisecond, minimumDigestCount},
			},
			// With two owners, the median is the lower p90.
			wantMedian: 1,
			checks: []check{
				{"slow", 1.5, true, "with two owners, the median is the lower p90"},
				{"fast", 1.5, false, "a normal owner blocks spread"},
			},
		},
		{
			name: "no samples does not block",
			checks: []check{
				{"any", 1.5, true, "no samples"},
			},
		},
		{
			name: "one owner with enough samples does not block",
			samples: []samples{
				{"a", time.Millisecond, minimumDigestCount},
				{"b", time.Second, minimumDigestCount - 1},
			},
			wantMedian: 1,
			checks: []check{
				{"a", 1.5, true, "fewer than two owners with enough samples"},
				{"b", 1.5, true, "fewer than two owners with enough samples"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newOwnerLatencies(time.Minute, time.Now)
			for _, s := range tc.samples {
				addSamples(o, s.owner, s.latency, s.count)
			}
			gate := o.snapshot()
			if tc.wantMedian == 0 {
				require.Zero(t, gate.median)
			} else {
				// The bucket midpoint is within 1/16 of the latency.
				require.InEpsilon(t, tc.wantMedian, gate.median, 1.0/16)
			}

			for _, c := range tc.checks {
				require.Equal(t, c.want, gate.allows(c.owner, c.factor), "%s: owner %s, factor %v", c.msg, c.owner, c.factor)
			}
		})
	}
}

func TestLatencyGateAllowsAtExactRatio(t *testing.T) {
	// The bucket midpoints do not give an exact ratio, so the gate has fixed values.
	gate := latencyGate{p90: map[string]float64{"slow": 10, "a": 1, "b": 1}, median: 1}
	require.True(t, gate.allows("slow", 10), "the comparison is at least, not more than")
	require.False(t, gate.allows("slow", math.Nextafter(10, math.Inf(1))), "a factor above the ratio blocks spread")
}

func TestOwnerLatencyWindowRotation(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "slow", 10*time.Millisecond, minimumDigestCount)
	addSamples(o, "a", time.Millisecond, minimumDigestCount)
	addSamples(o, "b", time.Millisecond, minimumDigestCount)
	require.True(t, o.snapshot().allows("a", 0.5))

	// In the next window, the gate uses the previous window until the current window has enough samples.
	clock.Advance(time.Minute)
	require.Empty(t, o.counts())
	gate := o.snapshot()
	require.True(t, gate.allows("slow", 1.5))
	require.False(t, gate.allows("a", 1.5))

	// New latency in the current window replaces the previous window.
	addSamples(o, "a", 20*time.Millisecond, minimumDigestCount)
	gate = o.snapshot()
	require.True(t, gate.allows("a", 1.5))
	require.False(t, gate.allows("b", 1.5))

	// After two idle windows, no samples remain, and snapshot removes the idle owners.
	clock.Advance(2 * time.Minute)
	require.True(t, o.snapshot().allows("b", 1.5), "no owner has enough samples")
	require.Zero(t, o.digests.Size())
}

func TestOwnerLatencyConcurrentRecord(t *testing.T) {
	// The clock does not move, so no window resets and record counts every sample.
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				o.record("a", time.Duration(i)*time.Microsecond)
				o.record("b", time.Millisecond)
				if i%50 == 0 {
					_ = o.snapshot()
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, map[string]uint64{"a": 8000, "b": 8000}, o.counts())
}

func TestOwnerLatencyRecordDoesNotAllocate(t *testing.T) {
	o := newOwnerLatencies(time.Minute, time.Now)
	o.record("a", time.Millisecond)
	allocs := testing.AllocsPerRun(1000, func() {
		o.record("a", time.Millisecond)
	})
	require.Zero(t, allocs)
}

func TestLatencyBucket(t *testing.T) {
	for _, tc := range []struct {
		latency time.Duration
		want    int
	}{
		{-time.Second, 0},
		{0, 0},
		{999 * time.Nanosecond, 0},
		{time.Microsecond, 1},
		{7 * time.Microsecond, 7},
		{8 * time.Microsecond, 8},
		{15 * time.Microsecond, 15},
		{16 * time.Microsecond, 16},
		{17 * time.Microsecond, 16},
		{18 * time.Microsecond, 17},
		{200 * time.Microsecond, 44},
		{time.Millisecond, 63},
		{time.Second, 143},
		{time.Minute, 190},
		{1<<27*time.Microsecond - time.Microsecond, latencyBucketCount - 1},
		{1 << 27 * time.Microsecond, latencyBucketCount - 1},
		{time.Duration(math.MaxInt64), latencyBucketCount - 1},
	} {
		t.Run(tc.latency.String(), func(t *testing.T) {
			require.Equal(t, tc.want, latencyBucket(tc.latency))
		})
	}
}

func TestLatencyBucketIsMonotonicAndAccurate(t *testing.T) {
	prev := 0
	for latency := time.Microsecond; latency < 1<<27*time.Microsecond; latency += latency/97 + time.Microsecond {
		b := latencyBucket(latency)
		require.GreaterOrEqual(t, b, prev, "latency %v", latency)
		prev = b

		// At and above 8µs, the bucket midpoint is within 1/16 of the latency.
		if latency >= 8*time.Microsecond {
			require.InEpsilon(t, float64(latency)/float64(time.Millisecond), latencyBucketMillis(b), 1.0/16, "latency %v", latency)
		}
	}

	// The midpoint of each bucket is in that bucket.
	for i := range latencyBucketCount {
		midpoint := time.Duration(latencyBucketMillis(i) * float64(time.Millisecond))
		require.Equal(t, i, latencyBucket(midpoint), "bucket %d", i)
	}
}

func TestLatencyQuantileOfKnownDistributions(t *testing.T) {
	uniform := make([]time.Duration, 0, 1000)
	for i := range 1000 {
		uniform = append(uniform, time.Duration(i+1)*time.Millisecond)
	}

	repeat := func(latency time.Duration, count int) []time.Duration {
		return slices.Repeat([]time.Duration{latency}, count)
	}

	for _, tc := range []struct {
		name    string
		samples []time.Duration
	}{
		{"uniform", uniform},
		{"constant", repeat(5*time.Millisecond, 100)},
		{"sub-millisecond constant", repeat(300*time.Microsecond, 100)},
		{"bimodal with p90 in the high mode", slices.Concat(repeat(time.Millisecond, 80), repeat(50*time.Millisecond, 20))},
		{"bimodal with p90 in the low mode", slices.Concat(repeat(2*time.Millisecond, 95), repeat(100*time.Millisecond, 5))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newOwnerLatencies(time.Minute, time.Now)
			for _, s := range tc.samples {
				o.record("a", s)
			}

			sorted := slices.Sorted(slices.Values(tc.samples))
			exact := sorted[int(math.Ceil(defaultHedgerQuantile*float64(len(sorted))))-1]
			got := o.snapshot().p90["a"]
			require.InEpsilon(t, float64(exact)/float64(time.Millisecond), got, 0.1)
		})
	}
}

func TestLatencyGateIsCachedForRefreshInterval(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "slow", 10*time.Millisecond, minimumDigestCount)
	addSamples(o, "a", time.Millisecond, minimumDigestCount)

	first := o.gate()
	require.True(t, first.allows("slow", 1.5))

	// New samples within the interval do not change the gate.
	addSamples(o, "a", time.Second, 10*minimumDigestCount)
	clock.Advance(latencyGateRefresh - time.Nanosecond)
	require.Same(t, first, o.gate())

	clock.Advance(time.Nanosecond)
	second := o.gate()
	require.NotSame(t, first, second)
	require.False(t, second.allows("slow", 1.5))
}

func TestLatencyGateRebuildRemovesIdleOwners(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "a", time.Millisecond, 1)
	clock.Advance(2 * time.Minute)
	_ = o.gate()
	require.Zero(t, o.digests.Size())
}

func TestOwnerDigestResetsWhenTimeJumpsMoreThanOneWindow(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "a", time.Millisecond, 3)
	clock.Advance(time.Minute)
	addSamples(o, "a", time.Millisecond, 2)

	d, ok := o.digests.Load("a")
	require.True(t, ok)
	epoch := o.epoch()
	require.EqualValues(t, 2, d.windowCount(epoch))
	require.EqualValues(t, 3, d.windowCount(epoch-1))

	clock.Advance(3 * time.Minute)
	epoch = o.epoch()
	require.Zero(t, d.windowCount(epoch), "no window is for the current epoch")
	require.Zero(t, d.windowCount(epoch-1), "no window is for the previous epoch")

	// The first sample in the new window resets the old counts of that window.
	addSamples(o, "a", time.Millisecond, 1)
	require.EqualValues(t, 1, d.windowCount(epoch))
}

func TestOwnerDigestDropsSampleForOlderWindow(t *testing.T) {
	d := newOwnerDigest(10)
	d.add(10, 5)
	d.add(12, 5)

	// Epochs 10 and 12 use the same window. The window is for epoch 12 now, so a late sample for epoch 10 is dropped.
	d.add(10, 5)
	require.EqualValues(t, 1, d.windowCount(12))
	require.Zero(t, d.windowCount(10))
}

func TestOwnerLatencyRecordAfterIdleRemoval(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "a", time.Millisecond, 1)

	clock.Advance(2 * time.Minute)
	_ = o.snapshot()
	require.Zero(t, o.digests.Size())

	// A record after the removal gets a new digest.
	o.record("a", time.Millisecond)
	require.Equal(t, map[string]uint64{"a": 1}, o.counts())
}

func TestLatencyGateWhileAnotherCallerRebuilds(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "a", time.Millisecond, minimumDigestCount)
	addSamples(o, "b", time.Millisecond, minimumDigestCount)

	cached := o.gate()
	require.Contains(t, cached.p90, "a")

	clock.Advance(latencyGateRefresh)
	o.rebuilding.Store(true)
	require.Same(t, cached, o.gate(), "a stale cached gate is used while another caller rebuilds")
	o.rebuilding.Store(false)
}

func TestFirstLatencyGateWaitsForBuilder(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "a", time.Millisecond, minimumDigestCount)
	addSamples(o, "b", time.Millisecond, minimumDigestCount)

	// The test is the builder. It holds the rebuild flag and has not stored a gate yet.
	o.rebuilding.Store(true)
	got := make(chan *latencyGate, 1)
	go func() { got <- o.gate() }()

	select {
	case <-got:
		t.Fatal("without a cached gate, gate must wait for the builder")
	case <-time.After(50 * time.Millisecond):
	}

	built := &cachedGate{gate: o.snapshot(), builtAt: clock.Now()}
	o.cached.Store(built)
	o.firstOnce.Do(func() { close(o.firstBuilt) })
	o.rebuilding.Store(false)

	select {
	case gate := <-got:
		require.Same(t, &built.gate, gate, "the waiting caller uses the gate of the builder")
	case <-time.After(5 * time.Second):
		t.Fatal("gate did not return after the builder finished")
	}
}

func TestOwnerLatenciesRejectsNonPositiveWindow(t *testing.T) {
	// MustBugf panics in tests. In production, newOwnerLatencies logs and uses spreadWindow.
	require.Panics(t, func() { newOwnerLatencies(0, time.Now) })
	require.Panics(t, func() { newOwnerLatencies(-time.Second, time.Now) })
}

// BenchmarkLatencyGate compares a cached gate with a full snapshot for 10 owners.
func BenchmarkLatencyGate(b *testing.B) {
	o := newOwnerLatencies(time.Minute, time.Now)
	for i := range 10 {
		addSamples(o, string(rune('a'+i)), time.Duration(i+1)*time.Millisecond, 1000)
	}
	b.Run("cached", func(b *testing.B) {
		for b.Loop() {
			_ = o.gate()
		}
	})
	b.Run("snapshot", func(b *testing.B) {
		for b.Loop() {
			_ = o.snapshot()
		}
	})
}

// BenchmarkOwnerLatencyRecord records from many goroutines into one owner.
func BenchmarkOwnerLatencyRecord(b *testing.B) {
	o := newOwnerLatencies(time.Minute, time.Now)
	o.record("a", time.Millisecond)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			o.record("a", time.Millisecond)
		}
	})
}
