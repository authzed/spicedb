package historicalresultsize_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestHistoryDerivesAcrossRevisions(t *testing.T) {
	c := querysignalstest.NewClock()
	o := querysignalstest.Owner(t, c, resultsize.New(), historicalresultsize.New())
	a, b := querysignalstest.View(t, o, "a", 1), querysignalstest.View(t, o, "a", 2)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}

	for range 4 {
		require.NoError(t, signal.Observe(a, resultsize.Input, k, resultsize.Observation{Count: 100, Started: c.Now()}))
	}

	exact, _ := signal.Lookup(b, resultsize.Ref, k)
	require.Equal(t, signal.Missing, exact.Availability)
	e, err := signal.Lookup(b, historicalresultsize.Ref, k)
	require.NoError(t, err)
	require.InDelta(t, 100.0, e.Value.Mean, 1e-9)
	require.True(t, e.Evidence.Historical)
	before := e.Value.Weight
	c.Advance(time.Minute)
	e, _ = signal.Lookup(b, historicalresultsize.Ref, k)
	require.InDelta(t, before*.5, e.Value.Weight, .2)
	require.InDelta(t, 100.0, e.Value.Mean, 1e-9)
	require.InDelta(t, 100.0, e.Value.Distribution.Quantile(.9), 1e-9)
	c.Advance(5 * time.Minute)
	e, _ = signal.Lookup(b, historicalresultsize.Ref, k)
	require.Equal(t, signal.Expired, e.Availability)
}

func TestDuplicateIncrementAndLateAge(t *testing.T) {
	c := querysignalstest.NewClock()
	s := historicalresultsize.NewAccumulator(c.Now)
	o := resultsize.Observation{Count: 100, Started: c.Now(), Update: signal.UpdateID{Epoch: 1, Sequence: 1}}
	require.NoError(t, s.Add(o))
	require.NoError(t, s.Add(o))
	e, status := s.Value(c.Now())
	require.Equal(t, signal.Available, status)
	require.Equal(t, uint64(1), e.Samples)
	c.Advance(6 * time.Minute)
	o.Update.Sequence++
	require.NoError(t, s.Add(o))
	_, status = s.Value(c.Now())
	require.Equal(t, signal.Expired, status)
}

func TestFractionalMixture(t *testing.T) {
	c := querysignalstest.NewClock()
	c.Advance(5 * time.Second) // Sample at bucket midpoint: the old bucket has exactly half weight.
	s := historicalresultsize.NewAccumulator(c.Now)

	for range 10 {
		require.NoError(t, s.Add(resultsize.Observation{Count: 0, Started: c.Now()}))
	}

	c.Advance(time.Minute)

	for range 10 {
		require.NoError(t, s.Add(resultsize.Observation{Count: 100, Started: c.Now()}))
	}

	e, _ := s.Value(c.Now())
	require.InDelta(t, 200.0/3, e.Mean, .01)
	require.InDelta(t, 1.0/3, e.ZeroFraction, .001)
	require.Greater(t, e.Distribution.Quantile(.5), 99.0)
	require.Less(t, e.Distribution.CDF(50), .34)
}

func TestConfigurableDecay(t *testing.T) {
	clock := querysignalstest.NewClock()
	descriptor, err := historicalresultsize.NewWithOptions(
		historicalresultsize.Options{BucketWidth: time.Second, Retention: time.Minute, HalfLife: time.Second},
	)
	require.NoError(t, err)
	owner := querysignalstest.Owner(t, clock, resultsize.New(), descriptor)
	v := querysignalstest.View(t, owner, "a", 1)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}

	for range 8 {
		require.NoError(t, signal.Observe(v, resultsize.Input, k, resultsize.Observation{Count: 6, Started: clock.Now()}))
	}

	clock.Advance(2 * time.Second)
	e, err := signal.Lookup(v, historicalresultsize.Ref, k)
	require.NoError(t, err)
	require.InDelta(t, 6.0, e.Value.Mean, 1e-9)
	require.Less(t, e.Value.Weight, 3.0)
	_, err = historicalresultsize.NewWithOptions(historicalresultsize.Options{HalfLife: -time.Second})
	require.Error(t, err)
}

func TestLateIncrementCannotExpireReplayProtectionEarly(t *testing.T) {
	c := querysignalstest.NewClock()
	old := c.Now()
	c.Advance(290 * time.Second)
	a := historicalresultsize.NewAccumulator(c.Now)
	young := resultsize.Observation{Count: 1, Started: c.Now(), Update: signal.UpdateID{Epoch: 1, Sequence: 1}}
	require.NoError(t, a.Add(young))

	for epoch := uint64(2); epoch < 10; epoch++ {
		o := young
		o.Update.Epoch = epoch
		require.NoError(t, a.Add(o))
	}

	// Completion order can differ from invocation start order on the same source.
	require.NoError(t, a.Add(resultsize.Observation{Count: 2, Started: old, Update: signal.UpdateID{Epoch: 1, Sequence: 2}}))
	c.Advance(20 * time.Second)
	require.NoError(t, a.Add(young))
	e, status := a.Value(c.Now())
	require.Equal(t, signal.Available, status)
	require.Equal(t, uint64(9), e.Samples)
}

func TestFreshEvidenceDoesNotInheritSchemaDiscount(t *testing.T) {
	c := querysignalstest.NewClock()
	a := historicalresultsize.NewAccumulator(c.Now)

	for range 2 {
		require.NoError(t, a.Add(resultsize.Observation{Count: 10, Started: c.Now()}))
	}

	a.(signal.Discountable).DiscountEvidence(.5)
	require.NoError(t, a.Add(resultsize.Observation{Count: 100, Started: c.Now()}))
	e, status := a.Value(c.Now())
	require.Equal(t, signal.Available, status)
	require.InDelta(t, 55.0, e.Mean, 1e-9)
	require.InDelta(t, 2.0, e.Weight, 1e-9)
}
