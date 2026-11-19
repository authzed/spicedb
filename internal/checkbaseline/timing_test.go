package checkbaseline

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type timingEngine struct {
	failAt   int
	calls    int
	observed bool
}

func (e *timingEngine) Name() string { return "fake" }
func (e *timingEngine) Close() error { return nil }
func (e *timingEngine) Check(ctx context.Context, _ Case) (Decision, error) {
	e.calls++
	if e.failAt > 0 && e.calls >= e.failAt {
		return Decision{}, fmt.Errorf("delayed failure")
	}
	e.observed = e.observed || recorder(ctx) != nil
	return Decision{Outcome: Allow}, nil
}

func TestTimingSamples(t *testing.T) {
	originalBenchtime := flag.Lookup("test.benchtime").Value.String()
	a, b := &timingEngine{}, &timingEngine{}
	samples, err := measurePair(t.Context(), []Engine{a, b}, Case{Expected: Decision{Outcome: Allow}}, 10, time.Second)
	require.Equal(t, originalBenchtime, flag.Lookup("test.benchtime").Value.String())
	require.NoError(t, err)
	for _, s := range samples {
		require.Len(t, s, 10)
		for _, v := range s {
			require.Positive(t, v.NSPerOp)
			require.Positive(t, v.Iterations)
		}
	}
	require.False(t, a.observed)
	require.False(t, b.observed)
	require.Equal(t, a.calls, b.calls)
}

func (e *timingEngine) Preparation() Sample { return Sample{} }

func TestPartialTimingSamplesSurviveFailure(t *testing.T) {
	originalBenchtime := flag.Lookup("test.benchtime").Value.String()
	a, b := &timingEngine{failAt: 550}, &timingEngine{}
	samples, err := measurePair(t.Context(), []Engine{a, b}, Case{Expected: Decision{Outcome: Allow}}, 10, time.Second)
	require.Equal(t, originalBenchtime, flag.Lookup("test.benchtime").Value.String())
	require.ErrorContains(t, err, "delayed failure")
	require.NotEmpty(t, samples[0])
	require.NotEmpty(t, samples[1])
}
