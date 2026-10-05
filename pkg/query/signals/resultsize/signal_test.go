package resultsize_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestCompletedSizesAndImmutableDistribution(t *testing.T) {
	c := querysignalstest.NewClock()
	v := querysignalstest.View(t, querysignalstest.Owner(t, c, resultsize.New()), "a", 1)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}

	for _, o := range []resultsize.Observation{
		{Count: 0, Status: signal.Complete},
		{Count: 10, Status: signal.Complete},
		{Count: 1, Status: signal.ConsumerStopped},
		{Status: signal.Errored},
	} {
		require.NoError(t, signal.Observe(v, resultsize.Input, k, o))
	}

	e, err := signal.Lookup(v, resultsize.Ref, k)
	require.NoError(t, err)
	require.Equal(t, uint64(2), e.Value.Samples)
	require.InDelta(t, 5.0, e.Value.Mean, 1e-9)
	require.InDelta(t, .5, e.Value.ZeroFraction, 1e-9)
	require.InDelta(t, 2.0, e.Evidence.EffectiveWeight, 1e-9)
	require.NoError(t, signal.Observe(v, resultsize.Input, k, resultsize.Observation{Count: 100, Status: signal.Complete}))
	require.InDelta(t, 10.0, e.Value.Distribution.Quantile(1), 1e-9)
	next, _ := signal.Lookup(v, resultsize.Ref, k)
	require.Equal(t, uint64(3), next.Value.Samples)
}

func TestCheckAndIncompleteNeverBecomeSizeSamples(t *testing.T) {
	c := querysignalstest.NewClock()
	v := querysignalstest.View(t, querysignalstest.Owner(t, c, resultsize.New()), "a", 1)

	for _, k := range []signal.ExecutionKey{{Identity: "x", Operation: signal.Check}, {Identity: "y", Operation: signal.IterResources}} {
		require.NoError(t, signal.Observe(v, resultsize.Input, k, resultsize.Observation{Status: signal.Canceled}))
		e, _ := signal.Lookup(v, resultsize.Ref, k)
		require.Equal(t, signal.Missing, e.Availability)
	}
}
