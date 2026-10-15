package frequency

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func newTestSketch(t testing.TB, maxCost int64) (*sketch, chan time.Time) {
	t.Helper()
	ticks := make(chan time.Time)
	s := newSketch(maxCost, ticks)
	t.Cleanup(s.Close)
	return s, ticks
}

func TestWidthFitsBudget(t *testing.T) {
	for _, tc := range []struct {
		maxCost int64
		width   uint64
	}{
		{1, minWidth},
		{1 << 20, 1 << 15},
		{8 << 20, 1 << 18},
	} {
		t.Run(fmt.Sprint(tc.maxCost), func(t *testing.T) {
			s, _ := newTestSketch(t, tc.maxCost)
			require.Equal(t, tc.width, s.width)
			require.Equal(t, rows*tc.width, uint64(len(s.counters)))
			require.Equal(t, doorkeeperBitsPerCounter*tc.width, uint64(64*len(s.doorkeeper)))
			if tc.width > minWidth {
				require.LessOrEqual(t, s.costBytes(), tc.maxCost)
				require.Greater(t, (&sketch{width: 2 * tc.width}).costBytes(), tc.maxCost,
					"width is the largest power of two in the budget")
			}
		})
	}
}

func TestDoorkeeperAbsorbsFirstTouch(t *testing.T) {
	s, _ := newTestSketch(t, 1<<20)

	require.Equal(t, uint64(1), s.Touch("cold"))
	require.Equal(t, uint32(0), s.sketchMin(hashKey("cold")), "the first touch must not update the sketch")
	require.True(t, s.doorkeeperHas(hashKey("cold")))
	require.Equal(t, uint64(1), s.estimate("cold"))
	require.Equal(t, uint64(1), s.Total(), "a doorkeeper-only touch counts in the total")

	require.Equal(t, uint64(2), s.Touch("cold"))
	require.Equal(t, uint32(1), s.sketchMin(hashKey("cold")))
	require.Equal(t, uint64(0), s.estimate("never-touched"))
}

func TestConservativeUpdateIncrementsOnlyMinimumRows(t *testing.T) {
	s, _ := newTestSketch(t, 1)
	h := hashKey("k")
	s.Touch("k")
	// Raise one row above the others, as a collision does.
	idx := s.index(h, 0)
	s.counters[idx].Store(5)
	require.Equal(t, uint64(2), s.Touch("k"))
	require.Equal(t, uint32(5), s.counters[idx].Load(), "a row above the minimum must not change")
	for i := uint64(1); i < rows; i++ {
		require.Equal(t, uint32(1), s.counters[s.index(h, i)].Load())
	}
}

func TestCountersSaturate(t *testing.T) {
	s, _ := newTestSketch(t, 1<<20)
	h := hashKey("hot")
	s.Touch("hot")
	for i := range uint64(rows) {
		s.counters[s.index(h, i)].Store(math.MaxUint32)
	}
	require.Equal(t, uint64(math.MaxUint32)+1, s.Touch("hot"))
	for i := range uint64(rows) {
		require.Equal(t, uint32(math.MaxUint32), s.counters[s.index(h, i)].Load(), "a counter must never wrap")
	}
}

func TestDecayHalvesCountsAndClearsDoorkeeper(t *testing.T) {
	s, _ := newTestSketch(t, 1<<20)
	for range 9 {
		s.Touch("hot")
	}
	s.Touch("cold")
	require.Equal(t, uint64(9), s.estimate("hot"))
	require.Equal(t, uint64(10), s.Total())

	s.decay()
	require.False(t, s.doorkeeperHas(hashKey("hot")))
	require.False(t, s.doorkeeperHas(hashKey("cold")))
	require.Equal(t, uint32(4), s.sketchMin(hashKey("hot")))
	require.Equal(t, uint64(5), s.Total())
	require.Equal(t, uint64(0), s.estimate("cold"), "a key seen once in the last window decays to 0")

	// The first touch after decay sets the doorkeeper and reads the decayed count plus 1.
	require.Equal(t, uint64(5), s.Touch("hot"))
	require.Equal(t, uint32(4), s.sketchMin(hashKey("hot")))
	require.Equal(t, uint64(6), s.Touch("hot"))

	for range 40 {
		s.decay()
	}
	require.Equal(t, uint64(0), s.estimate("hot"), "an idle key decays to 0")
	require.Equal(t, uint64(0), s.Total())
}

func TestTickDrivesDecay(t *testing.T) {
	ticks := make(chan time.Time)
	s := newSketch(1<<20, ticks)
	for range 8 {
		s.Touch("hot")
	}
	ticks <- time.Now()
	// Close waits for the goroutine, so the decay is complete when it returns.
	s.Close()
	require.Equal(t, uint32(3), s.sketchMin(hashKey("hot")))
	require.Equal(t, uint64(4), s.Total())
}

func TestSteadyRateReadsBetweenRateAndTwiceRate(t *testing.T) {
	s, _ := newTestSketch(t, 1<<20)
	const rate = 100
	for range 20 {
		var last uint64
		for range rate {
			last = s.Touch("steady")
		}
		require.LessOrEqual(t, last, uint64(2*rate))
		require.GreaterOrEqual(t, last, uint64(rate))
		s.decay()
	}
}

func TestCloseStopsGoroutine(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	est, err := NewEstimator(1<<20, time.Hour)
	require.NoError(t, err)
	est.Touch("k")
	est.Close()
	est.Close()
}

func TestTouchDoesNotAllocate(t *testing.T) {
	s, _ := newTestSketch(t, 1<<20)
	key := "document:foo#viewer"
	allocs := testing.AllocsPerRun(100, func() { s.Touch(key) })
	require.Zero(t, allocs)
}

func TestHugeBudgetCapsWidth(t *testing.T) {
	require.Equal(t, uint64(maxWidth), widthFor(math.MaxInt64))
}
