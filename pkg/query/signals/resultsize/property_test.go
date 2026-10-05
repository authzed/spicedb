package resultsize_test

import (
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestCompletedCountModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		c := querysignalstest.NewClock()
		v := querysignalstest.View(t, querysignalstest.Owner(t, c, resultsize.New()), "a", 1)
		k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}
		values := rapid.SliceOfN(rapid.IntRange(0, 100), 1, 100).Draw(rt, "values")
		total, zeros := 0, 0

		for _, n := range values {
			total += n

			if n == 0 {
				zeros++
			}

			require.NoError(rt, signal.Observe(v, resultsize.Input, k, resultsize.Observation{Count: safecast.MustConvert[uint64](n)}))
		}

		e, err := signal.Lookup(v, resultsize.Ref, k)
		require.NoError(rt, err)
		require.Equal(rt, uint64(len(values)), e.Value.Samples)
		require.InDelta(rt, float64(total)/float64(len(values)), e.Value.Mean, 1e-9)
		require.InDelta(rt, float64(zeros)/float64(len(values)), e.Value.ZeroFraction, 1e-9)
		require.LessOrEqual(rt, e.Value.Min, e.Value.Distribution.Quantile(.5))
		require.GreaterOrEqual(rt, e.Value.Max, e.Value.Distribution.Quantile(.5))
	})
}
