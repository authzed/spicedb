package frequency_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/frequency"
)

func TestTouchCountsPerKey(t *testing.T) {
	est, err := frequency.NewEstimator(1<<20, time.Minute)
	require.NoError(t, err)
	defer est.Close()

	require.Equal(t, uint64(1), est.Touch("document:foo#viewer"))
	require.Equal(t, uint64(2), est.Touch("document:foo#viewer"))
	require.Equal(t, uint64(1), est.Touch("document:bar#viewer"))
	require.Equal(t, uint64(3), est.Touch("document:foo#viewer"))
	require.Equal(t, uint64(4), est.Total())
}

func TestTouchIsRaceFree(t *testing.T) {
	est, err := frequency.NewEstimator(1<<20, time.Minute)
	require.NoError(t, err)
	defer est.Close()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				est.Touch("shared")
				est.Touch(string(rune('a' + i%26)))
			}
		}()
	}
	wg.Wait()
	require.Equal(t, uint64(1600), est.Total())
	// Concurrent conservative updates can lose increments. The count must still see most of the traffic.
	require.GreaterOrEqual(t, est.Touch("shared"), uint64(401))
}

func TestNewEstimatorRejectsBadArguments(t *testing.T) {
	_, err := frequency.NewEstimator(1<<20, 0)
	require.Error(t, err)

	// A small budget gives the minimum width.
	est, err := frequency.NewEstimator(1, time.Minute)
	require.NoError(t, err)
	est.Close()
}
