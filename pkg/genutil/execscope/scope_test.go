package execscope

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopeWaitsForLosingWork(t *testing.T) {
	scope := New()
	ctx := scope.Context(context.Background())
	finish := Begin(ctx)
	done := make(chan struct{})
	go func() { scope.Wait(); close(done) }()

	select {
	case <-done:
		t.Fatal("wait returned before registered work finished")
	default:
	}

	finish()
	<-done
	require.NotNil(t, Begin(context.Background()))
}

func TestScopeRecordsConcurrencyPolicy(t *testing.T) {
	scope := New()
	ctx := scope.Context(t.Context())
	RecordConcurrency(ctx, 4, 2, false)
	RecordConcurrency(ctx, 4, 1, true)
	stats := scope.Statistics()
	require.Equal(t, uint64(2), stats.SetChecks)
	require.Equal(t, uint16(2), stats.MaxEffectiveWidth)
	require.Equal(t, uint64(1), stats.SerialFallbacks)
	scope.ResetStatistics()
	require.Zero(t, scope.Statistics().SetChecks)
}
