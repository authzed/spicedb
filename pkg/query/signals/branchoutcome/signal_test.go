package branchoutcome_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestCompletedCheckOutcomes(t *testing.T) {
	c := querysignalstest.NewClock()
	v := querysignalstest.View(t, querysignalstest.Owner(t, c, branchoutcome.New()), "a", 1)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.Check}

	for _, o := range []branchoutcome.Observation{
		{Status: signal.Complete, Outcome: branchoutcome.Deny},
		{Status: signal.Complete, Outcome: branchoutcome.Grant},
		{Status: signal.Complete, Outcome: branchoutcome.Conditional},
		{Status: signal.Canceled},
		{Status: signal.Errored},
	} {
		require.NoError(t, signal.Observe(v, branchoutcome.Input, k, o))
	}

	e, err := signal.Lookup(v, branchoutcome.Ref, k)
	require.NoError(t, err)
	require.Equal(t, uint64(3), e.Value.Completed)
	require.Equal(t, uint64(5), e.Value.Attempted)
	require.InDelta(t, 1.0/3, e.Value.GrantFraction, 1e-12)
	require.Equal(t, uint64(1), e.Value.Conditional)
}
