package historicalresultsize_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
)

func TestConstantValueSurvivesAging(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		x := rapid.IntRange(0, 10000).Draw(rt, "value")
		age := time.Duration(rapid.IntRange(0, 240).Draw(rt, "age")) * time.Second
		var e resultsize.Estimate

		// Give each generated example its own clock, including during shrinking.
		synctest.Test(t, func(t *testing.T) {
			s := historicalresultsize.NewAccumulator(time.Now)

			for range 100 {
				require.NoError(t, s.Add(resultsize.Observation{Count: safecast.MustConvert[uint64](x), Started: time.Now()}))
			}

			time.Sleep(age)
			e, _ = s.Value(time.Now())
		})

		require.InDelta(rt, float64(x), e.Mean, 1e-8)
		require.InDelta(rt, float64(x), e.Distribution.Quantile(.9), 1e-9)
		require.GreaterOrEqual(rt, e.Weight, 1.0)
		require.LessOrEqual(rt, e.Weight, 100.0)
	})
}
