package signal_test

import (
	"math"
	"testing"

	"github.com/caio/go-tdigest/v4"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestDistributionRetainsIndependentSnapshot(t *testing.T) {
	digest, err := tdigest.New()

	require.NoError(t, err)
	require.NoError(t, digest.AddWeighted(10, 100))
	require.NoError(t, digest.AddWeighted(20, 100))
	snapshot := signal.DigestDistribution(digest, 10, 20)

	require.NoError(t, digest.AddWeighted(1000, 10000))
	require.InDelta(t, 10.0, snapshot.Quantile(0), 1e-9)
	require.InDelta(t, 20.0, snapshot.Quantile(1), 1e-9)
	require.InDelta(t, 15.0, snapshot.Quantile(0.5), 1)
	require.Zero(t, snapshot.CDF(9))
	require.InDelta(t, 1.0, snapshot.CDF(20), 1e-9)

	for _, q := range []float64{-1, 2, math.NaN()} {
		require.True(t, math.IsNaN(snapshot.Quantile(q)))
	}
}

func TestWeightedMixtureOwnsParts(t *testing.T) {
	low, err := tdigest.New()

	require.NoError(t, err)
	require.NoError(t, low.AddWeighted(1, 100))
	high, err := tdigest.New()

	require.NoError(t, err)
	require.NoError(t, high.AddWeighted(9, 100))
	parts := []signal.WeightedDistribution{
		{Distribution: signal.DigestDistribution(low, 1, 1), Weight: 1},
		{Distribution: signal.DigestDistribution(high, 9, 9), Weight: 3},
	}
	mixture := signal.Mixture(parts, 1, 9)
	parts[0].Weight = 1000

	require.InDelta(t, 0.25, mixture.CDF(5), 1e-9)
	require.InDelta(t, 9.0, mixture.Quantile(0.5), 1e-6)
}

func TestExecutionFootprintRoundTrip(t *testing.T) {
	footprint := signal.Footprint{
		Elements: []signal.Element{{Namespace: "doc", Relation: "view"}},
		Caveats:  []string{"allowed"},
	}
	key := signal.ExecutionKey{Dependencies: signal.EncodeFootprint(footprint)}

	require.Equal(t, footprint, key.Footprint())
	require.Empty(t, (signal.ExecutionKey{Dependencies: "invalid"}).Footprint())
}
