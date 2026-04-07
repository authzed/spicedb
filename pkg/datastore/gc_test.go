package datastore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/pkg/testutil"
)

// testRevision is a minimal Revision implementation for GC tests.
type testRevision struct{ id uint64 }

func (r testRevision) String() string              { return fmt.Sprintf("%d", r.id) }
func (r testRevision) Equal(o Revision) bool       { return r.id == o.(testRevision).id }
func (r testRevision) GreaterThan(o Revision) bool { return r.id > o.(testRevision).id }
func (r testRevision) LessThan(o Revision) bool    { return r.id < o.(testRevision).id }
func (r testRevision) ByteSortable() bool          { return true }

// Fake garbage collector that returns a new incremented revision each time
// TxIDBefore is called.
type fakeGC struct {
	lock sync.RWMutex

	metrics      gcMetrics // GUARDED_BY(lock)
	deleter      gcDeleter // GUARDED_BY(lock)
	lastRevision uint64    // GUARDED_BY(lock)
}

type gcMetrics struct {
	deleteBeforeTxCount    int
	deleteExpiredRelsCount int
}

type fakeGCStore struct {
	lock sync.RWMutex

	markedCompleteCount   int // GUARDED_BY(lock)
	resetGCCompletedCount int // GUARDED_BY(lock)

	fakeGC *fakeGC
}

func (f *fakeGCStore) BuildGarbageCollector(ctx context.Context) (GarbageCollector, error) {
	return f.fakeGC, nil
}

func (f *fakeGCStore) ReadyState(context.Context) (ReadyState, error) {
	return ReadyState{IsReady: true}, nil
}

func (f *fakeGCStore) HasGCRun() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.markedCompleteCount > 0
}

func (f *fakeGCStore) MarkGCCompleted() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.markedCompleteCount++
}

func (f *fakeGCStore) ResetGCCompleted() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.resetGCCompletedCount++
}

func newFakeGCStore(deleter gcDeleter) *fakeGCStore {
	return &fakeGCStore{
		fakeGC: &fakeGC{deleter: deleter},
	}
}

func (*fakeGC) Now(_ context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (gc *fakeGC) TxIDBefore(_ context.Context, _ time.Time) (Revision, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()
	gc.lastRevision++
	return testRevision{id: gc.lastRevision}, nil
}

func (gc *fakeGC) DeleteBeforeTx(_ context.Context, rev Revision) (DeletionCounts, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()
	gc.metrics.deleteBeforeTxCount++
	return gc.deleter.DeleteBeforeTx(rev.(testRevision).id)
}

func (gc *fakeGC) DeleteExpiredRels(_ context.Context) (int64, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()
	gc.metrics.deleteExpiredRelsCount++
	return gc.deleter.DeleteExpiredRels()
}

func (gc *fakeGC) Close() {}

var _ GarbageCollector = &fakeGC{}

// Allows specifying different deletion behaviors for tests
type gcDeleter interface {
	DeleteBeforeTx(revision uint64) (DeletionCounts, error)
	DeleteExpiredRels() (int64, error)
}

// Only error on specific revisions
type revisionErrorDeleter struct {
	errorOnRevisions []uint64
}

func (d revisionErrorDeleter) DeleteBeforeTx(revision uint64) (DeletionCounts, error) {
	if slices.Contains(d.errorOnRevisions, revision) {
		return DeletionCounts{}, fmt.Errorf("delete error")
	}
	return DeletionCounts{}, nil
}

func (d revisionErrorDeleter) DeleteExpiredRels() (int64, error) {
	return 0, nil
}

func alwaysErr() error {
	return errors.New("aaagh")
}

func TestGCFailureBackoff(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})
	localCounter := prometheus.NewCounter(gcFailureCounterConfig)
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(localCounter))

	errCh := make(chan error, 1)
	synctest.Test(t, func(t *testing.T) {
		duration := 1000 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), duration)
		t.Cleanup(func() {
			cancel()
		})
		go func() {
			errCh <- runGCOnIntervalWithBackoff(ctx, alwaysErr, 100*time.Second, 1*time.Minute, localCounter)
		}()
		time.Sleep(duration)
		synctest.Wait()
	})
	require.Error(t, <-errCh)

	metrics, err := reg.Gather()
	require.NoError(t, err)
	var mf *promclient.MetricFamily
	for _, metric := range metrics {
		if metric.GetName() == "spicedb_datastore_gc_failure_total" {
			mf = metric
			break
		}
	}
	// We expect about 5 failures; the behavior of the library means that there's some wiggle room here.
	// (owing to the jitter in the backoff)
	// we should see failures at 100s, 200s, 400s, 800s
	// but depending on jitter this could end up being squished down such that we get 5 failures.
	// Experimentally, we see 5 most often and 4 sometimes, so asserting greater than 3 works here.
	require.Greater(t, *(mf.GetMetric()[0].Counter.Value), 3.0, "did not see expected number of backoffs")
}

// Ensure the garbage collector interval is reset after recovering from an
// error. The garbage collector should not continue to use the exponential
// backoff interval that is activated on error.
func TestGCFailureBackoffReset(t *testing.T) {
	gc := newFakeGCStore(revisionErrorDeleter{
		// Error on revisions 1 - 5, giving the exponential
		// backoff enough time to fail the test if the interval
		// is not reset properly.
		errorOnRevisions: []uint64{1, 2, 3, 4, 5},
	})

	errCh := make(chan error, 1)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			errCh <- StartGarbageCollector(ctx, gc, 10*time.Millisecond, 10*time.Second, 1*time.Minute)
		}()
		// The garbage collector should fail 5 times starting with 10ms interval,
		// which should take ~160ms to complete. after that, it should resume
		// completing a revision every 10ms, which should get us at least 20
		// successful iterations with a 500ms total execution time.
		// If the interval is not reset, it should blow past the 500ms timer
		// after only completing ~3 iterations.
		time.Sleep(500 * time.Millisecond)
		cancel()
		synctest.Wait()
	})

	require.Error(t, <-errCh)

	gc.lock.Lock()
	defer gc.lock.Unlock()

	require.Greater(t, gc.markedCompleteCount, 20, "Next interval was not reset with backoff")
}

// preemptingDeleter returns ErrGCPreempted from the selected methods.
type preemptingDeleter struct {
	preemptDeleteBeforeTx    bool
	preemptDeleteExpiredRels bool
}

func (d *preemptingDeleter) DeleteBeforeTx(_ uint64) (DeletionCounts, error) {
	if d.preemptDeleteBeforeTx {
		return DeletionCounts{}, ErrGCPreempted
	}
	return DeletionCounts{}, nil
}

func (d *preemptingDeleter) DeleteExpiredRels() (int64, error) {
	if d.preemptDeleteExpiredRels {
		return 0, ErrGCPreempted
	}
	return 0, nil
}

func TestGCPreemption(t *testing.T) {
	cases := []struct {
		name                         string
		preemptDeleteBeforeTx        bool
		preemptDeleteExpiredRels     bool
		expectDeleteBeforeTxCount    int
		expectDeleteExpiredRelsCount int
	}{
		{
			name:                         "preempted on DeleteBeforeTx skips DeleteExpiredRels",
			preemptDeleteBeforeTx:        true,
			expectDeleteBeforeTxCount:    1,
			expectDeleteExpiredRelsCount: 0,
		},
		{
			name:                         "preempted on DeleteExpiredRels",
			preemptDeleteExpiredRels:     true,
			expectDeleteBeforeTxCount:    1,
			expectDeleteExpiredRelsCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gc := newFakeGCStore(&preemptingDeleter{
				preemptDeleteBeforeTx:    tc.preemptDeleteBeforeTx,
				preemptDeleteExpiredRels: tc.preemptDeleteExpiredRels,
			})

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)

				err := RunGarbageCollection(ctx, gc, 10*time.Second)
				require.NoError(t, err, "preemption should not be treated as an error")
			})

			gc.lock.Lock()
			defer gc.lock.Unlock()
			require.Positive(t, gc.markedCompleteCount, "preemption should still mark GC as completed")

			gc.fakeGC.lock.Lock()
			defer gc.fakeGC.lock.Unlock()
			require.Equal(t, tc.expectDeleteBeforeTxCount, gc.fakeGC.metrics.deleteBeforeTxCount)
			require.Equal(t, tc.expectDeleteExpiredRelsCount, gc.fakeGC.metrics.deleteExpiredRelsCount)
		})
	}
}
