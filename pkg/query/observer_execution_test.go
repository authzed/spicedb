package query

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type executionRecorder struct {
	mu     sync.Mutex
	events []ExecutionObservation // GUARDED_BY(mu)
}

func (r *executionRecorder) ObserveExecution(e ExecutionObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func TestExecutionSequenceCompletion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stop  bool
		want  signal.Terminal
		count uint64
	}{{"exhausted", false, signal.Complete, 2}, {"stopped", true, signal.ConsumerStopped, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			r := &executionRecorder{}
			ctx := NewLocalContext(t.Context(), WithExecutionObserver(r))
			it := NewFixedIterator(*MustPathFromString("doc:d#read@user:a"), *MustPathFromString("doc:d#read@user:b"))
			ctx.MarkAsOperation(it, OperationIterSubjects) // Observe the raw iterator; API deduplication otherwise exhausts it eagerly.
			seq, err := ctx.IterSubjects(it, NewObject("doc", "d"), NoObjectFilter())
			require.NoError(t, err)
			require.Empty(t, r.events)

			for _, err := range seq {
				require.NoError(t, err)

				if tc.stop {
					break
				}
			}

			require.Len(t, r.events, 1)
			require.Equal(t, tc.want, r.events[0].Status)
			require.Equal(t, tc.count, r.events[0].Count)
		})
	}
}

func TestExecutionCheckNilIsCompletedDeny(t *testing.T) {
	r := &executionRecorder{}
	ctx := NewLocalContext(t.Context(), WithExecutionObserver(r))
	_, err := ctx.Check(NewFixedIterator(), NewObject("doc", "d"), NewObject("user", "a").WithEllipses())
	require.NoError(t, err)
	require.Len(t, r.events, 1)
	require.Equal(t, signal.Complete, r.events[0].Status)
	require.Equal(t, uint64(1), r.events[0].Denied)
}

func TestConcurrentInvocationsHaveUniqueTokens(t *testing.T) {
	r := &executionRecorder{}
	ctx := NewLocalContext(context.Background(), WithExecutionObserver(r))
	it := NewFixedIterator()
	ctx.MarkAsOperation(it, OperationCheck)
	var wg sync.WaitGroup

	for range 20 {
		wg.Go(func() {
			_, err := ctx.Check(it, NewObject("doc", "d"), NewObject("user", "a").WithEllipses())
			require.NoError(t, err)
		})
	}

	wg.Wait()
	ids := map[uint64]bool{}

	for _, e := range r.events {
		require.False(t, ids[e.Invocation])
		ids[e.Invocation] = true
	}

	require.Len(t, ids, 20)
}

type failingExecutor struct {
	LocalExecutor
	construction bool
}

func (f failingExecutor) Check(*Context, Iterator, Object, ObjectAndRelation) (*Path, error) {
	return nil, errors.New("check failed")
}

func (f failingExecutor) IterSubjects(*Context, Iterator, Object, ObjectType) (PathSeq, error) {
	if f.construction {
		return nil, errors.New("construction failed")
	}

	return func(yield func(*Path, error) bool) { yield(nil, errors.New("sequence failed")) }, nil
}

func TestExecutionErrorsAndCancellation(t *testing.T) {
	for _, construction := range []bool{true, false} {
		r := &executionRecorder{}
		ctx := NewQueryContext(t.Context(), failingExecutor{construction: construction}, WithExecutionObserver(r))
		it := NewFixedIterator()
		ctx.MarkAsOperation(it, OperationIterSubjects)
		seq, err := ctx.IterSubjects(it, NewObject("doc", "d"), NoObjectFilter())

		if construction {
			require.Error(t, err)
		} else {
			require.NoError(t, err)

			for _, err := range seq {
				require.Error(t, err)
			}
		}

		require.Len(t, r.events, 1)
		require.Equal(t, signal.Errored, r.events[0].Status)
	}

	r := &executionRecorder{}
	ctx := NewQueryContext(t.Context(), failingExecutor{}, WithExecutionObserver(r))
	_, err := ctx.Check(NewFixedIterator(), NewObject("doc", "d"), NewObject("user", "a").WithEllipses())
	require.Error(t, err)
	require.Zero(t, r.events[0].Denied)
	std, cancel := context.WithCancel(t.Context())
	cancel()
	r = &executionRecorder{}
	ctx = NewLocalContext(std, WithExecutionObserver(r))
	_, err = ctx.Check(NewFixedIterator(), NewObject("doc", "d"), NewObject("user", "a").WithEllipses())
	require.NoError(t, err)
	require.Equal(t, signal.Canceled, r.events[0].Status)
}

func TestExecutionBatchDenominators(t *testing.T) {
	r := &executionRecorder{}
	ctx := NewLocalContext(t.Context(), WithExecutionObserver(r))
	it := NewFixedIterator(*MustPathFromString("doc:d#read@user:a"))
	ctx.MarkAsOperation(it, OperationCheck)
	_, err := ctx.CheckManySubjects(
		it,
		NewObject("doc", "d"),
		[]ObjectAndRelation{NewObject("user", "a").WithEllipses(), NewObject("user", "b").WithEllipses()},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.events[0].Granted)
	require.Equal(t, uint64(1), r.events[0].Denied)
	require.Equal(t, "many-subjects", r.events[0].Key.Mode)
}
