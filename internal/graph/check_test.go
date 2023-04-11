package graph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAsyncDispatch(t *testing.T) {
	testCases := []struct {
		numRequests      uint16
		concurrencyLimit uint16
	}{
		{1, 1},
		{10, 1},
		{1, 50},
		{50, 50},
		{1000, 10},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%d/%d", tc.numRequests, tc.concurrencyLimit), func(t *testing.T) {
			require := require.New(t)

			ctx := context.Background()

			l := &sync.Mutex{}
			letFinish := sync.NewCond(l)
			var dispatchedCount uint16
			var completedCount uint16

			reqs := make([]int, 0, tc.numRequests)

			for i := 0; i < int(tc.numRequests); i++ {
				reqs = append(reqs, i)
			}

			channel := make(chan CheckResult, tc.numRequests)

			dispatchAllAsync(ctx, currentRequestContext{}, reqs,
				func(ctx context.Context, crc currentRequestContext, child int) CheckResult {
					l.Lock()
					defer l.Unlock()
					dispatchedCount++
					letFinish.Wait()
					completedCount++
					return noMembers()
				}, channel, tc.concurrencyLimit)

			require.Eventually(func() bool {
				l.Lock()
				defer l.Unlock()

				return (tc.numRequests >= tc.concurrencyLimit && dispatchedCount == tc.concurrencyLimit) ||
					(tc.numRequests < tc.concurrencyLimit && dispatchedCount == tc.numRequests)
			}, 1*time.Second, 1*time.Millisecond)

			l.Lock()
			require.Equal(uint16(0), completedCount)
			l.Unlock()

			letFinish.Signal()

			require.Eventually(func() bool {
				l.Lock()
				defer l.Unlock()

				return completedCount == 1
			}, 10*time.Millisecond, 10*time.Microsecond)

			require.Eventually(func() bool {
				l.Lock()
				defer l.Unlock()

				letFinish.Broadcast()
				return tc.numRequests == dispatchedCount && tc.numRequests == completedCount
			}, 1*time.Second, 1*time.Millisecond)
		})
	}
}
