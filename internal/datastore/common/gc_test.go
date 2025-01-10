package common

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"

	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Fake garbage collector that returns a new incremented revision each time
// TxIDBefore is called.
type fakeGC struct {
	lastRevision uint64
	deleter      gcDeleter
	metrics      gcMetrics
	lock         sync.RWMutex
	wasLocked    bool
	wasUnlocked  bool
}

type gcMetrics struct {
	deleteBeforeTxCount    int
	markedCompleteCount    int
	resetGCCompletedCount  int
	deleteExpiredRelsCount int
}

func newFakeGC(deleter gcDeleter) fakeGC {
	return fakeGC{
		lastRevision: 0,
		deleter:      deleter,
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

func (*fakeGC) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{
		Message: "Ready",
		IsReady: true,
	}, nil
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

func (gc *fakeGC) HasGCRun() bool {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	return gc.metrics.markedCompleteCount > 0
}

func (gc *fakeGC) MarkGCCompleted() {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.metrics.markedCompleteCount++
}

func (gc *fakeGC) ResetGCCompleted() {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	gc.metrics.resetGCCompletedCount++
}

func (gc *fakeGC) GetMetrics() gcMetrics {
	gc.lock.Lock()
	defer gc.lock.Unlock()

	return gc.metrics
}

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

func TestGCFailureBackoff(t *testing.T) {
	localCounter := prometheus.NewCounter(gcFailureCounterConfig)
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(localCounter))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		gc := newFakeGC(alwaysErrorDeleter{})
		require.Error(t, startGarbageCollectorWithMaxElapsedTime(ctx, &gc, 100*time.Millisecond, 1*time.Second, 1*time.Nanosecond, 1*time.Minute, localCounter))
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()

	metrics, err := reg.Gather()
	require.NoError(t, err)
	var mf *promclient.MetricFamily
	for _, metric := range metrics {
		if metric.GetName() == "spicedb_datastore_gc_failure_total" {
			mf = metric
		}
	}
	require.Greater(t, *(mf.GetMetric()[0].Counter.Value), 100.0, "MaxElapsedTime=1ns did not cause backoff to get ignored")

	localCounter = prometheus.NewCounter(gcFailureCounterConfig)
	reg = prometheus.NewRegistry()
	require.NoError(t, reg.Register(localCounter))
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() {
		gc := newFakeGC(alwaysErrorDeleter{})
		require.Error(t, startGarbageCollectorWithMaxElapsedTime(ctx, &gc, 100*time.Millisecond, 0, 1*time.Second, 1*time.Minute, localCounter))
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()

	metrics, err = reg.Gather()
	require.NoError(t, err)
	for _, metric := range metrics {
		if metric.GetName() == "spicedb_datastore_gc_failure_total" {
			mf = metric
		}
	}
	require.Less(t, *(mf.GetMetric()[0].Counter.Value), 3.0, "MaxElapsedTime=0 should have not caused backoff to get ignored")
}

// Ensure the garbage collector interval is reset after recovering from an
// error. The garbage collector should not continue to use the exponential
// backoff interval that is activated on error.
func TestGCFailureBackoffReset(t *testing.T) {
	gc := newFakeGC(revisionErrorDeleter{
		// Error on revisions 1 - 5, giving the exponential
		// backoff enough time to fail the test if the interval
		// is not reset properly.
		errorOnRevisions: []uint64{1, 2, 3, 4, 5},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		interval := 10 * time.Millisecond
		window := 10 * time.Second
		timeout := 1 * time.Minute

		require.Error(t, StartGarbageCollector(ctx, &gc, interval, window, timeout))
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()

	// The next interval should have been reset after recovering from the error.
	// If it is not reset, the last exponential backoff interval will not give
	// the GC enough time to run.
	require.Greater(t, gc.GetMetrics().markedCompleteCount, 20, "Next interval was not reset with backoff")
}

func TestGCUnlockOnTimeout(t *testing.T) {
	gc := newFakeGC(alwaysErrorDeleter{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		interval := 10 * time.Millisecond
		window := 10 * time.Second
		timeout := 1 * time.Millisecond

		require.Error(t, StartGarbageCollector(ctx, &gc, interval, window, timeout))
	}()

	time.Sleep(30 * time.Millisecond)
	require.False(t, gc.HasGCRun(), "GC should not have run")

	gc.lock.Lock()
	defer gc.lock.Unlock()

	require.True(t, gc.wasLocked, "GC should have been locked")
	require.True(t, gc.wasUnlocked, "GC should have been unlocked")
}
