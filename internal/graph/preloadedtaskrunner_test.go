package graph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/testutil"

	"github.com/stretchr/testify/require"

	"go.uber.org/goleak"
)

func TestPreloadedTaskRunnerCompletesAllTasks(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := newPreloadedTaskRunner(context.Background(), 2, 5)
	wg := sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		i := i
		tr.add(func(ctx context.Context) error {
			time.Sleep(time.Duration(i*10) * time.Millisecond)
			wg.Done()
			return nil
		})
	}

	tr.start()

	testutil.RequireWithin(t, func(t *testing.T) {
		wg.Wait()
	}, 5*time.Second)
}

func TestPreloadedTaskRunnerCancelsEarlyDueToError(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := newPreloadedTaskRunner(ctx, 3, 10)
	completed := sync.Map{}

	for i := 0; i < 10; i++ {
		i := i
		tr.add(func(ctx context.Context) error {
			if i == 1 {
				return fmt.Errorf("some error")
			}

			time.Sleep(time.Duration(i*50) * time.Millisecond)
			completed.Store(i, true)
			return nil
		})
	}

	tr.start()

	time.Sleep(1 * time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		if _, ok := completed.Load(i); ok {
			count++
		}
	}

	require.GreaterOrEqual(t, count, 1)
	require.Less(t, count, 9)
}

func TestPreloadedTaskRunnerCancelsEarlyDueToCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := newPreloadedTaskRunner(ctx, 3, 10)
	completed := sync.Map{}

	for i := 0; i < 10; i++ {
		i := i
		tr.add(func(ctx context.Context) error {
			if i == 1 {
				cancel()
				return nil
			}

			time.Sleep(time.Duration(i*50) * time.Millisecond)
			completed.Store(i, true)
			return nil
		})
	}

	tr.start()

	time.Sleep(1 * time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		if _, ok := completed.Load(i); ok {
			count++
		}
	}

	require.GreaterOrEqual(t, count, 1)
	require.Less(t, count, 9)
}

func TestPreloadedTaskRunnerReturnsError(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := newPreloadedTaskRunner(ctx, 3, 10)
	completed := sync.Map{}

	for i := 0; i < 10; i++ {
		i := i
		tr.add(func(ctx context.Context) error {
			if i == 1 {
				return fmt.Errorf("some error")
			}

			time.Sleep(time.Duration(i*50) * time.Millisecond)
			completed.Store(i, true)
			return nil
		})
	}

	time.Sleep(1 * time.Second)

	err := tr.startAndWait()
	require.ErrorContains(t, err, "some error")

	count := 0
	for i := 0; i < 10; i++ {
		if _, ok := completed.Load(i); ok {
			count++
		}
	}

	require.GreaterOrEqual(t, count, 1)
	require.Less(t, count, 9)
}
