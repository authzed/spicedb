package branchoutcome_test

import (
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestOutcomeModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		v := querysignalstest.View(t, querysignalstest.Owner(t, branchoutcome.New()), "a", 1)
		k := signal.ExecutionKey{Identity: "x", Operation: signal.Check}
		counts := [3]uint64{}
		n := rapid.IntRange(1, 100).Draw(rt, "attempts")

		for range n {
			x := rapid.IntRange(0, 4).Draw(rt, "outcome")
			o := branchoutcome.Observation{}

			if x < 3 {
				o.Outcome = safecast.MustConvert[branchoutcome.Outcome](x)
				counts[x]++
			} else {
				o.Status = signal.Canceled
			}

			require.NoError(rt, signal.Observe(v, branchoutcome.Input, k, o))
		}

		e, err := signal.Lookup(v, branchoutcome.Ref, k)
		require.NoError(rt, err)
		require.Equal(rt, safecast.MustConvert[uint64](n), e.Value.Attempted)
		require.Equal(rt, counts[0], e.Value.Denied)
		require.Equal(rt, counts[1], e.Value.Granted)
		require.Equal(rt, counts[2], e.Value.Conditional)
	})
}
