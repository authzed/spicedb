package common

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"

	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// Allows specifying different deletion behaviors for tests
type gcDeleter interface {
	DeleteBeforeTx(revision int64) (DeletionCounts, error)
}

// Always error trying to perform a delete
type alwaysErrorDeleter struct{}

func (alwaysErrorDeleter) DeleteBeforeTx(_ int64) (DeletionCounts, error) {
	return DeletionCounts{}, fmt.Errorf("delete error")
}

// Only error on specific revisions
type revisionErrorDeleter struct {
	errorOnRevisions []int64
}

func (d revisionErrorDeleter) DeleteBeforeTx(revision int64) (DeletionCounts, error) {
	if slices.Contains(d.errorOnRevisions, revision) {
		return DeletionCounts{}, fmt.Errorf("delete error")
	}

	return DeletionCounts{}, nil
}

// Fake garbage collector that returns a new incremented revision each time
// TxIDBefore is called.
type fakeGC struct {
	lastRevision          int64
	deleter               gcDeleter
	deleteBeforeTxCount   int
	markedCompleteCount   int
	resetGCCompletedCount int
}

func newFakeGC(deleter gcDeleter) fakeGC {
	return fakeGC{
		lastRevision: 0,
		deleter:      deleter,
	}
}

func (t fakeGC) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{
		Message: "Ready",
		IsReady: true,
	}, nil
}

func (t fakeGC) Now(_ context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (t *fakeGC) TxIDBefore(_ context.Context, _ time.Time) (datastore.Revision, error) {
	t.lastRevision++

	rev := revision.NewFromDecimal(decimal.NewFromInt(t.lastRevision))

	return rev, nil
}

func (t *fakeGC) DeleteBeforeTx(_ context.Context, rev datastore.Revision) (DeletionCounts, error) {
	t.deleteBeforeTxCount++

	revInt := rev.(revision.Decimal).Decimal.IntPart()

	return t.deleter.DeleteBeforeTx(revInt)
}

func (t fakeGC) HasGCRun() bool {
	return t.markedCompleteCount > 0
}

func (t *fakeGC) MarkGCCompleted() {
	t.markedCompleteCount++
}

func (t *fakeGC) ResetGCCompleted() {
	t.resetGCCompletedCount++
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
		errorOnRevisions: []int64{1}, // Error only on revision 1
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
	require.Greater(t, gc.markedCompleteCount, 10, "Next interval was not reset with backoff")
}
