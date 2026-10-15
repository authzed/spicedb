package remote

import (
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
		d.mu.Lock()
		defer d.mu.Unlock()
		d.rotate(epoch)
		if c := d.current.Count(); c > 0 {
			out[owner] = c
		}
		return true
	})
	return out
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
				{"slow", 10, true, "the comparison is at least, not more than"},
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
			require.InDelta(t, tc.wantMedian, gate.median, 0.01)
			for _, c := range tc.checks {
				require.Equal(t, c.want, gate.allows(c.owner, c.factor), "%s: owner %s, factor %v", c.msg, c.owner, c.factor)
			}
		})
	}
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
	o := newOwnerLatencies(time.Minute, time.Now)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				o.record("a", time.Millisecond)
				o.record("b", time.Millisecond)
				_ = o.snapshot()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, map[string]uint64{"a": 800, "b": 800}, o.counts())
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
	d.mu.Lock()
	require.EqualValues(t, 2, d.current.Count())
	require.EqualValues(t, 3, d.previous.Count())
	d.mu.Unlock()

	clock.Advance(3 * time.Minute)
	d.mu.Lock()
	d.rotate(o.epoch())
	require.Zero(t, d.current.Count(), "the current window is reset")
	require.Zero(t, d.previous.Count(), "the previous window is reset")
	require.Equal(t, o.epoch(), d.epoch)
	d.mu.Unlock()
}

func TestOwnerLatencyRecordRetriesRemovedDigest(t *testing.T) {
	o := newOwnerLatencies(time.Minute, time.Now)
	stale := &ownerDigest{removed: true}
	o.digests.Store("a", stale)

	done := make(chan struct{})
	go func() {
		defer close(done)
		o.record("a", time.Millisecond)
	}()

	// record retries while the map holds the removed digest.
	select {
	case <-done:
		t.Fatal("record must not finish while the removed digest is in the map")
	case <-time.After(50 * time.Millisecond):
	}

	o.digests.Delete("a")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("record did not retry with a new digest")
	}

	fresh, ok := o.digests.Load("a")
	require.True(t, ok)
	require.NotSame(t, stale, fresh)
	require.Equal(t, map[string]uint64{"a": 1}, o.counts())
}

func TestLatencyGateWhileAnotherCallerRebuilds(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	o := newOwnerLatencies(time.Minute, clock.Now)
	addSamples(o, "a", time.Millisecond, minimumDigestCount)
	addSamples(o, "b", time.Millisecond, minimumDigestCount)

	o.rebuilding.Store(true)
	fresh := o.gate()
	require.Contains(t, fresh.p90, "a", "without a cached gate, the caller builds a snapshot")
	require.Nil(t, o.cached.Load(), "a caller that does not rebuild does not cache")

	o.rebuilding.Store(false)
	cached := o.gate()
	clock.Advance(latencyGateRefresh)
	o.rebuilding.Store(true)
	require.Same(t, cached, o.gate(), "a stale cached gate is used while another caller rebuilds")
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
