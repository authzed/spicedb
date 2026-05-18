package datastore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	internalmetrics "github.com/authzed/spicedb/internal/metrics"
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
		goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)
	})
	localCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: gcFailureCounterOpts.Namespace,
		Subsystem: gcFailureCounterOpts.Subsystem,
		Name:      gcFailureCounterOpts.Name,
		Help:      gcFailureCounterOpts.Help,
	})
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(localCounter))

	errCh := make(chan error, 1)
	synctest.Test(t, func(t *testing.T) {
		duration := 1000 * time.Second
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			errCh <- runGCOnIntervalWithBackoff(ctx, alwaysErr, 100*time.Second, 1*time.Minute, localCounter)
		}()
		time.Sleep(duration)
		cancel()
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

func TestRunGCOnIntervalWithBackoffResetsAfterSuccess(t *testing.T) {
	factory := internalmetrics.NewRecordingFactory()
	failureCounter := factory.Counter(internalmetrics.Opts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_failure_reset_test_total",
		Help:      "test",
	})

	var callCount atomic.Int32
	errCh := make(chan error, 1)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			errCh <- runGCOnIntervalWithBackoff(ctx, func() error {
				if callCount.Add(1) == 1 {
					return errors.New("first call fails")
				}
				return nil
			}, 10*time.Millisecond, 1*time.Minute, failureCounter)
		}()

		// Allow enough virtual time for one failure and several successful runs.
		time.Sleep(100 * time.Millisecond)
		cancel()
		synctest.Wait()
	})

	require.Error(t, <-errCh)
	require.GreaterOrEqual(t, callCount.Load(), int32(3), "expected repeated runs after a successful retry")
	require.InEpsilon(t, 1.0, factory.CounterValue("spicedb", "datastore", "gc_failure_reset_test_total"), 1e-9)
}

func TestRegisterGCMetricsRepeatedRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()

	first, err := RegisterGCMetrics(registry)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := RegisterGCMetrics(registry)
	require.NoError(t, err)
	require.Len(t, second, len(first))

	for index := range first {
		require.Same(t, first[index], second[index])
	}
}

func TestRegisterGCMetricsWithNilRegisterer(t *testing.T) {
	// Passing nil should use DefaultRegisterer
	collectors, err := RegisterGCMetrics(nil)
	// Should succeed without error
	require.NoError(t, err)
	require.NotEmpty(t, collectors)
	require.Len(t, collectors, 6)
}

func TestRegisterGCMetricsReturnsCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	collectors, err := RegisterGCMetrics(registry)

	require.NoError(t, err)
	require.Len(t, collectors, 6)

	// Verify we got the expected collector types
	_, isHistogram := collectors[0].(prometheus.Histogram)
	require.True(t, isHistogram, "First collector should be a Histogram")

	for i := 1; i < len(collectors); i++ {
		_, isCounter := collectors[i].(prometheus.Counter)
		require.True(t, isCounter, "Collector %d should be a Counter", i)
	}
}

func TestRegisterGCMetricsAssignsGlobalMetricsFromRegisteredCollectors(t *testing.T) {
	gcMetricsMu.Lock()
	originalHistogram := gcDurationHistogram
	originalRelationships := gcRelationshipsCounter
	originalExpired := gcExpiredRelationshipsCounter
	originalTransactions := gcTransactionsCounter
	originalNamespaces := gcNamespacesCounter
	originalFailure := gcFailureCounter
	gcMetricsMu.Unlock()

	t.Cleanup(func() {
		gcMetricsMu.Lock()
		defer gcMetricsMu.Unlock()
		gcDurationHistogram = originalHistogram
		gcRelationshipsCounter = originalRelationships
		gcExpiredRelationshipsCounter = originalExpired
		gcTransactionsCounter = originalTransactions
		gcNamespacesCounter = originalNamespaces
		gcFailureCounter = originalFailure
	})

	registry := prometheus.NewRegistry()
	collectors, err := RegisterGCMetrics(registry)
	require.NoError(t, err)
	require.Len(t, collectors, 6)

	gcMetricsMu.Lock()
	registeredHistogram, ok := gcDurationHistogram.(prometheus.Histogram)
	require.True(t, ok)
	registeredRelationships, ok := gcRelationshipsCounter.(prometheus.Counter)
	require.True(t, ok)
	registeredExpired, ok := gcExpiredRelationshipsCounter.(prometheus.Counter)
	require.True(t, ok)
	registeredTransactions, ok := gcTransactionsCounter.(prometheus.Counter)
	require.True(t, ok)
	registeredNamespaces, ok := gcNamespacesCounter.(prometheus.Counter)
	require.True(t, ok)
	registeredFailure, ok := gcFailureCounter.(prometheus.Counter)
	require.True(t, ok)
	gcMetricsMu.Unlock()

	require.Equal(t, collectors[0], registeredHistogram)
	require.Equal(t, collectors[1], registeredRelationships)
	require.Equal(t, collectors[2], registeredExpired)
	require.Equal(t, collectors[3], registeredTransactions)
	require.Equal(t, collectors[4], registeredNamespaces)
	require.Equal(t, collectors[5], registeredFailure)

	registeredHistogram.Observe(0.25)
	registeredRelationships.Add(1)
	registeredExpired.Add(1)
	registeredTransactions.Add(1)
	registeredNamespaces.Add(1)
	registeredFailure.Add(1)

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies)
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
			errCh <- StartGarbageCollector(ctx, gc, 10*time.Millisecond, 10*time.Second, 1*time.Minute)
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

func TestSetMetricsFactoryWithRecordingFactory(t *testing.T) {
	original := internalmetrics.NewPrometheusFactory(nil)
	t.Cleanup(func() {
		SetMetricsFactory(original)
	})

	recordingFactory := internalmetrics.NewRecordingFactory()
	SetMetricsFactory(recordingFactory)

	// Verify that the metrics are set to the recording factory's implementations
	require.NotNil(t, gcDurationHistogram)
	require.NotNil(t, gcRelationshipsCounter)
	require.NotNil(t, gcFailureCounter)

	// Should not panic when using recording factory metrics
	gcDurationHistogram.Observe(1.0)
	gcRelationshipsCounter.Add(1)
	gcFailureCounter.Add(1)
}

func TestSetMetricsFactoryWithNoopFactory(t *testing.T) {
	original := internalmetrics.NewPrometheusFactory(nil)
	t.Cleanup(func() {
		SetMetricsFactory(original)
	})

	SetMetricsFactory(internalmetrics.NoopFactory{})

	// Verify that the metrics are set (even if they're no-op)
	require.NotNil(t, gcDurationHistogram)
	require.NotNil(t, gcRelationshipsCounter)
	require.NotNil(t, gcFailureCounter)

	// Should not panic when using noop factory metrics
	gcDurationHistogram.Observe(1.0)
	gcRelationshipsCounter.Add(1)
	gcFailureCounter.Add(1)
}

func TestSetMetricsFactoryWithNil(t *testing.T) {
	original := internalmetrics.NewPrometheusFactory(nil)
	t.Cleanup(func() {
		SetMetricsFactory(original)
	})

	// Passing nil should default to NoopFactory
	SetMetricsFactory(nil)

	// Verify that the metrics are set (to noop implementations)
	require.NotNil(t, gcDurationHistogram)
	require.NotNil(t, gcRelationshipsCounter)
	require.NotNil(t, gcFailureCounter)

	// Should not panic when using noop factory metrics
	gcDurationHistogram.Observe(1.0)
	gcRelationshipsCounter.Add(1)
	gcFailureCounter.Add(1)
}
