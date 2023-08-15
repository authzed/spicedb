package taskrunner

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

func TestTaskRunnerCompletesAllTasks(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := NewTaskRunner(context.Background(), 2)
	completed := sync.Map{}

	for i := 0; i < 5; i++ {
		i := i
		tr.Schedule(func(ctx context.Context) error {
			time.Sleep(time.Duration(i*10) * time.Millisecond)
			completed.Store(i, true)
			return nil
		})
	}

	testutil.RequireWithin(t, func(t *testing.T) {
		err := tr.Wait()
		require.NoError(t, err)
	}, 5*time.Second)

	for i := 0; i < 5; i++ {
		vle, ok := completed.Load(i)
		require.True(t, ok)
		require.True(t, vle.(bool))
	}
}

func TestTaskRunnerCancelsEarlyDueToError(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := NewTaskRunner(ctx, 3)
	completed := sync.Map{}

	for i := 0; i < 10; i++ {
		i := i
		tr.Schedule(func(ctx context.Context) error {
			if i == 1 {
				return fmt.Errorf("some error")
			}

			time.Sleep(time.Duration(i*50) * time.Millisecond)
			completed.Store(i, true)
			return nil
		})
	}

	testutil.RequireWithin(t, func(t *testing.T) {
		err := tr.Wait()
		require.Error(t, err)
		require.Contains(t, err.Error(), "some error")
	}, 5*time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		if _, ok := completed.Load(i); ok {
			count++
		}
	}

	require.Less(t, count, 9)
}

func TestTaskRunnerCancelsEarlyDueToCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := NewTaskRunner(ctx, 3)
	completed := sync.Map{}

	for i := 0; i < 10; i++ {
		i := i
		tr.Schedule(func(ctx context.Context) error {
			if i == 1 {
				cancel()
				return nil
			}

			time.Sleep(time.Duration(i*50) * time.Millisecond)
			completed.Store(i, true)
			return nil
		})
	}

	testutil.RequireWithin(t, func(t *testing.T) {
		err := tr.Wait()
		require.Error(t, err)
		require.Contains(t, err.Error(), "canceled")
	}, 5*time.Second)

	count := 0
	for i := 0; i < 10; i++ {
		if _, ok := completed.Load(i); ok {
			count++
		}
	}

	require.Less(t, count, 9)
}

func TestTaskRunnerDoesNotBlockOnQueuing(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	completed := false
	tr := NewTaskRunner(ctx, 1)
	tr.Schedule(func(ctx context.Context) error {
		// Schedule another task which should not block.
		tr.Schedule(func(ctx context.Context) error {
			completed = true
			return nil
		})
		return nil
	})

	testutil.RequireWithin(t, func(t *testing.T) {
		err := tr.Wait()
		require.NoError(t, err)
		require.True(t, completed)
	}, 5*time.Second)
}
