package contextfield_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestFieldStability(t *testing.T) {
	c := querysignalstest.NewClock()
	v := querysignalstest.View(t, querysignalstest.Owner(t, c, contextfield.New()), "a", 1)

	for _, o := range []contextfield.Observation{{Present: true, Fingerprint: "a"}, {Present: true, Fingerprint: "a"}, {Present: false}} {
		require.NoError(t, signal.Observe(v, contextfield.Input, "ip", o))
	}

	e, err := signal.Lookup(v, contextfield.Ref, "ip")
	require.NoError(t, err)
	require.Equal(t, uint64(3), e.Value.Samples)
	require.Equal(t, uint64(1), e.Value.Missing)
	require.True(t, e.Value.AllEqual)
	require.InDelta(t, 1.0, e.Value.DominantShareLowerBound, 1e-9)
	require.NoError(t, signal.Observe(v, contextfield.Input, "ip", contextfield.Observation{Present: true, Fingerprint: "b"}))
	next, _ := signal.Lookup(v, contextfield.Ref, "ip")
	require.False(t, next.Value.AllEqual)
	require.True(t, e.Value.AllEqual)
}

func TestMissingOnlyAndFingerprintOverflow(t *testing.T) {
	c := querysignalstest.NewClock()
	v := querysignalstest.View(t, querysignalstest.Owner(t, c, contextfield.New()), "a", 1)
	require.NoError(t, signal.Observe(v, contextfield.Input, "ip", contextfield.Observation{}))
	e, _ := signal.Lookup(v, contextfield.Ref, "ip")
	require.False(t, e.Value.AllEqual)

	for i := range 17 {
		require.NoError(
			t,
			signal.Observe(v, contextfield.Input, "ip", contextfield.Observation{Present: true, Fingerprint: string(rune('a' + i))}),
		)
	}

	e, _ = signal.Lookup(v, contextfield.Ref, "ip")
	require.Equal(t, uint64(1), e.Value.Overflow)
	require.InDelta(t, 1.0/17, e.Value.DominantShareLowerBound, 1e-12)
}
