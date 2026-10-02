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
	now time.Time
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

func TestLatencyGateMath(t *testing.T) {
	o := newOwnerLatencies(time.Minute, time.Now)
	addSamples(o, "slow", 10*time.Millisecond, minimumDigestCount)
	addSamples(o, "a", time.Millisecond, minimumDigestCount)
	addSamples(o, "b", time.Millisecond, minimumDigestCount)
	addSamples(o, "few", time.Second, minimumDigestCount-1)

	gate := o.snapshot()
	require.InDelta(t, 1, gate.median, 0.01, "the median uses only owners with enough samples")
	require.True(t, gate.allows("slow", 1.5), "an elevated owner allows spread")
	require.False(t, gate.allows("a", 1.5), "a normal owner blocks spread")
	require.False(t, gate.allows("few", 1.5), "an owner without enough samples blocks spread")
	require.False(t, gate.allows("unknown", 1.5))
	require.True(t, gate.allows("slow", 10), "the comparison is at least, not more than")
	require.False(t, gate.allows("slow", 10.5))
}

func TestLatencyGateSubMillisecondResolution(t *testing.T) {
	o := newOwnerLatencies(time.Minute, time.Now)
	addSamples(o, "slow", 600*time.Microsecond, minimumDigestCount)
	addSamples(o, "a", 200*time.Microsecond, minimumDigestCount)
	addSamples(o, "b", 200*time.Microsecond, minimumDigestCount)
	gate := o.snapshot()
	require.True(t, gate.allows("slow", 1.5))
	require.False(t, gate.allows("a", 1.5))
}

func TestLatencyGateTwoOwners(t *testing.T) {
	o := newOwnerLatencies(time.Minute, time.Now)
	addSamples(o, "slow", 2*time.Millisecond, minimumDigestCount)
	addSamples(o, "fast", time.Millisecond, minimumDigestCount)
	gate := o.snapshot()
	require.True(t, gate.allows("slow", 1.5), "with two owners, the median is the lower p90")
	require.False(t, gate.allows("fast", 1.5))
}

func TestLatencyGateFewerThanTwoOwnersDoesNotBlock(t *testing.T) {
	o := newOwnerLatencies(time.Minute, time.Now)
	require.True(t, o.snapshot().allows("any", 1.5), "no samples")

	addSamples(o, "a", time.Millisecond, minimumDigestCount)
	addSamples(o, "b", time.Second, minimumDigestCount-1)
	gate := o.snapshot()
	require.True(t, gate.allows("a", 1.5))
	require.True(t, gate.allows("b", 1.5))
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

	// After two idle windows, no samples remain, and idle owners are removed.
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
