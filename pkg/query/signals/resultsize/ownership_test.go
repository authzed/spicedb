package resultsize_test

import (
	"sync"
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestAccumulatorClonesCanLearnConcurrently(t *testing.T) {
	original := resultsize.NewAccumulator()

	for i := range 100 {
		require.NoError(t, original.Add(resultsize.Observation{Count: safecast.MustConvert[uint64](i % 7)}))
	}

	cloned := original.Clone()
	errors := make(chan error, 2)
	var workers sync.WaitGroup

	for _, accumulator := range []signal.Accumulator[resultsize.Observation, resultsize.Estimate]{original, cloned} {
		workers.Go(func() {
			for i := range 2000 {
				if err := accumulator.Add(resultsize.Observation{Count: safecast.MustConvert[uint64](i % 17)}); err != nil {
					errors <- err
					return
				}
			}
		})
	}

	workers.Wait()
	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}

	a, _ := original.Value(querysignalstest.NewClock().Now())
	b, _ := cloned.Value(querysignalstest.NewClock().Now())

	require.Equal(t, uint64(2100), a.Samples)
	require.Equal(t, a.Samples, b.Samples)
}

func TestSparseDigestHonorsResidentBudget(t *testing.T) {
	accumulator := resultsize.NewAccumulator()

	// Compression 100 reserves two 1,000-element slices before the first sample.
	require.GreaterOrEqual(t, accumulator.Size(), int64(16000))
	r, err := signals.NewRegistry(resultsize.New())

	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{MaxEntryBytes: 2048})

	require.NoError(t, err)
	v := querysignalstest.View(t, o, "schema", 1)
	key := signal.ExecutionKey{Identity: "read", Operation: signal.IterSubjects}

	require.NoError(t, signal.Observe(v, resultsize.Input, key, resultsize.Observation{Count: 1}))
	e, err := signal.Lookup(v, resultsize.Ref, key)

	require.NoError(t, err)
	require.False(t, e.IsAvailable())
}
