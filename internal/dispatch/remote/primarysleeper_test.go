package remote

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrimarySleeper_SleepWithZeroWaitTime(t *testing.T) {
	t.Parallel()
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   0,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		start := time.Now()
		sleeper.sleep(ctx)
		elapsed = time.Since(start)
	})

	require.Equal(t, elapsed, 0*time.Millisecond)
}

func TestPrimarySleeper_SleepWithPositiveWaitTime(t *testing.T) {
	t.Parallel()
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		start := time.Now()
		sleeper.sleep(ctx)
		elapsed = time.Since(start)
	})

	require.Equal(t, elapsed, waitTime)
}

func TestPrimarySleeper_SleepWithContextCancellation(t *testing.T) {
	t.Parallel()
	waitTime := 100 * time.Millisecond
	cancelTime := 5 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		go func() {
			time.Sleep(cancelTime)
			cancel()
		}()

		sleeper.sleep(ctx)
		elapsed = time.Since(start)
	})

	require.Equal(t, elapsed, cancelTime)
}

func TestPrimarySleeper_SleepWithContextTimeout(t *testing.T) {
	t.Parallel()
	waitTime := 100 * time.Millisecond
	cancelTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), cancelTime)
		defer cancel()
		start := time.Now()
		sleeper.sleep(ctx)
		elapsed = time.Since(start)
	})

	require.Equal(t, elapsed, cancelTime)
}

func TestPrimarySleeper_CancelSleep(t *testing.T) {
	t.Parallel()
	waitTime := 100 * time.Millisecond
	cancelTime := 5 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		start := time.Now()
		go func() {
			time.Sleep(cancelTime)
			sleeper.cancelSleep()
		}()

		sleeper.sleep(ctx)
		elapsed = time.Since(start)
		synctest.Wait()
	})

	require.Equal(t, elapsed, cancelTime)
}

func TestPrimarySleeper_CancelSleepBeforeSleep(t *testing.T) {
	t.Parallel()
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		sleeper.cancelSleep()
		start := time.Now()
		sleeper.sleep(ctx)
		elapsed = time.Since(start)
	})

	require.Equal(t, elapsed, waitTime)
}

func TestPrimarySleeper_MultipleCancelSleep(t *testing.T) {
	t.Parallel()
	waitTime := 50 * time.Millisecond
	cancelTime := 5 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		start := time.Now()
		go func() {
			time.Sleep(cancelTime)
			sleeper.cancelSleep()
			sleeper.cancelSleep()
			sleeper.cancelSleep()
		}()

		sleeper.sleep(ctx)
		elapsed = time.Since(start)
		synctest.Wait()
	})

	require.Equal(t, elapsed, cancelTime)
}

func TestPrimarySleeper_ConcurrentCancelSleep(t *testing.T) {
	t.Parallel()
	waitTime := 50 * time.Millisecond
	cancelTime := 5 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
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
		elapsed = time.Since(start)
		synctest.Wait()
	})

	require.Equal(t, elapsed, cancelTime)
}

func TestPrimarySleeper_SleepTwice(t *testing.T) {
	t.Parallel()
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var firstElapsed time.Duration
	var secondElapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		start := time.Now()
		sleeper.sleep(ctx)
		firstElapsed = time.Since(start)

		start = time.Now()
		sleeper.sleep(ctx)
		secondElapsed = time.Since(start)
	})

	require.Equal(t, firstElapsed, waitTime)
	require.Equal(t, secondElapsed, waitTime)
}

func TestPrimarySleeper_SleepConcurrently(t *testing.T) {
	t.Parallel()
	waitTime := 20 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var wg sync.WaitGroup
	results := make(chan time.Duration, 3)

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				start := time.Now()
				sleeper.sleep(ctx)
				results <- time.Since(start)
			}(i)
		}

		wg.Wait()
		close(results)
		synctest.Wait()
	})

	for elapsed := range results {
		require.Equal(t, elapsed, waitTime)
	}
}

func TestPrimarySleeper_EdgeCaseZeroDurationContext(t *testing.T) {
	t.Parallel()
	waitTime := 10 * time.Millisecond
	sleeper := &primarySleeper{
		reqKey:     "test",
		waitTime:   waitTime,
		cancelFunc: func() {},
		lock:       sync.Mutex{},
	}

	var elapsed time.Duration
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now())
		defer cancel()

		start := time.Now()
		sleeper.sleep(ctx)
		elapsed = time.Since(start)
		synctest.Wait()
	})

	require.Equal(t, elapsed, 0*time.Millisecond)
}
