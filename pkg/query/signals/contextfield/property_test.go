package contextfield_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestObservationModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		v := querysignalstest.View(t, querysignalstest.Owner(t, contextfield.New()), "a", 1)
		values := rapid.SliceOfN(rapid.IntRange(-1, 20), 1, 100).Draw(rt, "values")
		freq := map[int]int{}
		missing := 0

		for _, x := range values {
			if x < 0 {
				missing++
			} else {
				freq[x]++
			}

			require.NoError(
				rt,
				signal.Observe(v, contextfield.Input, "field", contextfield.Observation{Present: x >= 0, Fingerprint: fmt.Sprint(x)}),
			)
		}

		e, err := signal.Lookup(v, contextfield.Ref, "field")
		require.NoError(rt, err)
		require.Equal(rt, uint64(len(values)), e.Value.Samples)
		require.Equal(rt, uint64(missing), e.Value.Missing)
		require.Equal(rt, len(freq) == 1, e.Value.AllEqual)
		maxCount := 0

		for _, n := range freq {
			maxCount = max(maxCount, n)
		}

		if len(values) > missing {
			require.LessOrEqual(rt, e.Value.DominantShareLowerBound, float64(maxCount)/float64(len(values)-missing)+1e-12)
		}
	})
}
