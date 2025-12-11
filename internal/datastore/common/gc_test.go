package common

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

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

// Fake garbage collector that returns a new incremented revision each time
// TxIDBefore is called.
type fakeGC struct {
	lock sync.RWMutex

	metrics      gcMetrics // GUARDED_BY(lock)
	deleter      gcDeleter // GUARDED_BY(lock)
	lastRevision uint64    // GUARDED_BY(lock)
	wasLocked    bool      // GUARDED_BY(lock)
	wasUnlocked  bool      // GUARDED_BY(lock)
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

func (f *fakeGCStore) ReadyState(context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{
		IsReady: true,
	}, nil
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
		sync.RWMutex{},
		0,
		0,
		&fakeGC{
			lastRevision: 0,
			deleter:      deleter,
		},
	}
}

func (gc *fakeGC) LockForGCRun(ctx context.Context) (bool, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.wasLocked = true
	return true, nil
}

func (gc *fakeGC) UnlockAfterGCRun() error {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.wasUnlocked = true
	return nil
}

func (*fakeGC) Now(_ context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (gc *fakeGC) TxIDBefore(_ context.Context, _ time.Time) (datastore.Revision, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.lastRevision++

	rev := revisions.NewForTransactionID(gc.lastRevision)

	return rev, nil
}

func (gc *fakeGC) DeleteBeforeTx(_ context.Context, rev datastore.Revision) (DeletionCounts, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.metrics.deleteBeforeTxCount++

	revInt := rev.(revisions.TransactionIDRevision).TransactionID()

	return gc.deleter.DeleteBeforeTx(revInt)
}

func (gc *fakeGC) DeleteExpiredRels(_ context.Context) (int64, error) {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.metrics.deleteExpiredRelsCount++

	return gc.deleter.DeleteExpiredRels()
}

func (gc *fakeGC) GetMetrics() gcMetrics {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	return gc.metrics
}

func (gc *fakeGC) Close() {}

var _ GarbageCollector = &fakeGC{}

// Allows specifying different deletion behaviors for tests
type gcDeleter interface {
	DeleteBeforeTx(revision uint64) (DeletionCounts, error)
	DeleteExpiredRels() (int64, error)
}

// Always error trying to perform a delete
type alwaysErrorDeleter struct{}

func (alwaysErrorDeleter) DeleteBeforeTx(_ uint64) (DeletionCounts, error) {
	return DeletionCounts{}, fmt.Errorf("delete error")
}

func (alwaysErrorDeleter) DeleteExpiredRels() (int64, error) {
	return 0, fmt.Errorf("delete error")
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
		goleak.VerifyNone(t)
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
			errCh <- runOnIntervalWithBackoff(ctx, alwaysErr, 100*time.Second, 1*time.Minute, localCounter)
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
	fmt.Println("failures")
	fmt.Println(*(mf.GetMetric()[0].Counter.Value))
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
			interval := 10 * time.Millisecond
			window := 10 * time.Second
			timeout := 1 * time.Minute

			errCh <- StartGarbageCollector(ctx, gc, interval, window, timeout)
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

func TestGCUnlockOnTimeout(t *testing.T) {
	gc := newFakeGCStore(alwaysErrorDeleter{})

	errCh := make(chan error, 1)
	hasRunChan := make(chan bool, 1)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(func() {
			cancel()
		})
		go func() {
			interval := 10 * time.Millisecond
			window := 10 * time.Second
			timeout := 1 * time.Minute

			errCh <- StartGarbageCollector(ctx, gc, interval, window, timeout)
		}()
		time.Sleep(30 * time.Millisecond)
		hasRunChan <- gc.HasGCRun()
		cancel()
		synctest.Wait()
	})
	require.Error(t, <-errCh)
	require.False(t, <-hasRunChan, "GC should not have run because it should always be erroring.")

	// TODO: should this be inside the goroutine as well?
	gc.fakeGC.lock.Lock()
	defer gc.fakeGC.lock.Unlock()

	require.True(t, gc.fakeGC.wasLocked, "GC should have been locked")
	require.True(t, gc.fakeGC.wasUnlocked, "GC should have been unlocked")
}
