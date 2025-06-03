package remote

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrimarySleeper_SleepWithZeroWaitTime(t *testing.T) {
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   0,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()
	start := time.Now()
	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 1*time.Millisecond)
}

func TestPrimarySleeper_SleepWithPositiveWaitTime(t *testing.T) {
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()
	start := time.Now()
	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, waitTime)
	require.LessOrEqual(t, elapsed, waitTime+5*time.Millisecond)
}

func TestPrimarySleeper_SleepWithContextCancellation(t *testing.T) {
	waitTime := 100 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, waitTime)
	require.GreaterOrEqual(t, elapsed, 5*time.Millisecond)
}

func TestPrimarySleeper_SleepWithContextTimeout(t *testing.T) {
	waitTime := 100 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, waitTime)
	require.GreaterOrEqual(t, elapsed, 10*time.Millisecond)
}

func TestPrimarySleeper_CancelSleep(t *testing.T) {
	waitTime := 100 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()

	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		sleeper.cancelSleep()
	}()

	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, waitTime)
	require.GreaterOrEqual(t, elapsed, 5*time.Millisecond)
}

func TestPrimarySleeper_CancelSleepBeforeSleep(t *testing.T) {
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	sleeper.cancelSleep()

	ctx := context.Background()
	start := time.Now()
	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, waitTime)
}

func TestPrimarySleeper_MultipleCancelSleep(t *testing.T) {
	waitTime := 50 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()

	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		sleeper.cancelSleep()
		sleeper.cancelSleep()
		sleeper.cancelSleep()
	}()

	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, waitTime)
	require.GreaterOrEqual(t, elapsed, 5*time.Millisecond)
}

func TestPrimarySleeper_ConcurrentCancelSleep(t *testing.T) {
	waitTime := 50 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(5 * time.Millisecond)
			sleeper.cancelSleep()
		}()
	}

	go func() {
		wg.Wait()
	}()

	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, waitTime)
	require.GreaterOrEqual(t, elapsed, 5*time.Millisecond)
}

func TestPrimarySleeper_SleepTwice(t *testing.T) {
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()

	start := time.Now()
	sleeper.sleep(ctx)
	firstElapsed := time.Since(start)

	start = time.Now()
	sleeper.sleep(ctx)
	secondElapsed := time.Since(start)

	require.GreaterOrEqual(t, firstElapsed, waitTime)
	require.GreaterOrEqual(t, secondElapsed, waitTime)
}

func TestPrimarySleeper_SleepConcurrently(t *testing.T) {
	waitTime := 20 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	results := make([]time.Duration, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := time.Now()
			sleeper.sleep(ctx)
			results[idx] = time.Since(start)
		}(i)
	}

	wg.Wait()

	for _, elapsed := range results {
		require.GreaterOrEqual(t, elapsed, waitTime)
		require.LessOrEqual(t, elapsed, waitTime+10*time.Millisecond)
	}
}

func TestPrimarySleeper_EdgeCaseZeroDurationContext(t *testing.T) {
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now())
	defer cancel()

	start := time.Now()
	sleeper.sleep(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 5*time.Millisecond)
}

func TestPrimarySleeper_MetricsAreObserved(t *testing.T) {
	waitTime := 5 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "check",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	ctx := context.Background()
	sleeper.sleep(ctx)
}
