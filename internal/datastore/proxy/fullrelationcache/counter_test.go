package fullrelationcache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy/fullrelationcache"
)

func TestAccessCounterCountsPerKey(t *testing.T) {
	est, err := fullrelationcache.NewAccessCounter(1<<20, time.Minute)
	require.NoError(t, err)
	defer est.Close()

	require.Equal(t, uint64(1), est.Touch("document:foo#viewer"))
	require.Equal(t, uint64(2), est.Touch("document:foo#viewer"))
	require.Equal(t, uint64(1), est.Touch("document:bar#viewer"))
	require.Equal(t, uint64(3), est.Touch("document:foo#viewer"))
}

func TestAccessCounterIsRaceFree(t *testing.T) {
	est, err := fullrelationcache.NewAccessCounter(1<<20, time.Minute)
	require.NoError(t, err)
	defer est.Close()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				est.Touch("shared")
			}
		}()
	}
	wg.Wait()
	// The cache can refuse admission, which restarts a count. The final touch must still see most of the traffic.
	require.GreaterOrEqual(t, est.Touch("shared"), uint64(401))
}
