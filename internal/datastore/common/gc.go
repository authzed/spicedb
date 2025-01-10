package common

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	gcDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_duration_seconds",
		Help:      "The duration of datastore garbage collection.",
		Buckets:   []float64{0.01, 0.1, 0.5, 1, 5, 10, 25, 60, 120},
	})

	gcRelationshipsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_relationships_total",
		Help:      "The number of stale relationships deleted by the datastore garbage collection.",
	})

	gcExpiredRelationshipsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_expired_relationships_total",
		Help:      "The number of expired relationships deleted by the datastore garbage collection.",
	})

	gcTransactionsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_transactions_total",
		Help:      "The number of stale transactions deleted by the datastore garbage collection.",
	})

	gcNamespacesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_namespaces_total",
		Help:      "The number of stale namespaces deleted by the datastore garbage collection.",
	})

	gcFailureCounterConfig = prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_failure_total",
		Help:      "The number of failed runs of the datastore garbage collection.",
	}
	gcFailureCounter = prometheus.NewCounter(gcFailureCounterConfig)
)

// RegisterGCMetrics registers garbage collection metrics to the default
// registry.
func RegisterGCMetrics() error {
	for _, metric := range []prometheus.Collector{
		gcDurationHistogram,
		gcRelationshipsCounter,
		gcTransactionsCounter,
		gcNamespacesCounter,
		gcFailureCounter,
	} {
		if err := prometheus.Register(metric); err != nil {
			return err
		}
	}

	return nil
}

// GarbageCollector represents any datastore that supports external garbage
// collection.
type GarbageCollector interface {
	// HasGCRun returns true if a garbage collection run has been completed.
	HasGCRun() bool

	// MarkGCCompleted marks that a garbage collection run has been completed.
	MarkGCCompleted()

	// ResetGCCompleted resets the state of the garbage collection run.
	ResetGCCompleted()

	// LockForGCRun attempts to acquire a lock for garbage collection. This lock
	// is typically done at the datastore level, to ensure that no other nodes are
	// running garbage collection at the same time.
	LockForGCRun(ctx context.Context) (bool, error)

	// UnlockAfterGCRun releases the lock after a garbage collection run.
	// NOTE: this method does not take a context, as the context used for the
	// reset of the GC run can be canceled/timed out and the unlock will still need to happen.
	UnlockAfterGCRun() error

	// ReadyState returns the current state of the datastore.
	ReadyState(context.Context) (datastore.ReadyState, error)

	// Now returns the current time from the datastore.
	Now(context.Context) (time.Time, error)

	// TxIDBefore returns the highest transaction ID before the provided time.
	TxIDBefore(context.Context, time.Time) (datastore.Revision, error)

	// DeleteBeforeTx deletes all data before the provided transaction ID.
	DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (DeletionCounts, error)

	// DeleteExpiredRels deletes all relationships that have expired.
	DeleteExpiredRels(ctx context.Context) (int64, error)
}

// DeletionCounts tracks the amount of deletions that occurred when calling
// DeleteBeforeTx.
type DeletionCounts struct {
	Relationships int64
	Transactions  int64
	Namespaces    int64
}

func (g DeletionCounts) MarshalZerologObject(e *zerolog.Event) {
	e.
		Int64("relationships", g.Relationships).
		Int64("transactions", g.Transactions).
		Int64("namespaces", g.Namespaces)
}

var MaxGCInterval = 60 * time.Minute

// StartGarbageCollector loops forever until the context is canceled and
// performs garbage collection on the provided interval.
func StartGarbageCollector(ctx context.Context, gc GarbageCollector, interval, window, timeout time.Duration) error {
	return startGarbageCollectorWithMaxElapsedTime(ctx, gc, interval, window, 0, timeout, gcFailureCounter)
}

func startGarbageCollectorWithMaxElapsedTime(ctx context.Context, gc GarbageCollector, interval, window, maxElapsedTime, timeout time.Duration, failureCounter prometheus.Counter) error {
	backoffInterval := backoff.NewExponentialBackOff()
	backoffInterval.InitialInterval = interval
	backoffInterval.MaxInterval = max(MaxGCInterval, interval)
	backoffInterval.MaxElapsedTime = maxElapsedTime
	backoffInterval.Reset()

	nextInterval := interval

	log.Ctx(ctx).Info().
		Dur("interval", nextInterval).
		Msg("datastore garbage collection worker started")

	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().
				Msg("shutting down datastore garbage collection worker")
			return ctx.Err()

		case <-time.After(nextInterval):
			log.Ctx(ctx).Info().
				Dur("interval", nextInterval).
				Dur("window", window).
				Dur("timeout", timeout).
				Msg("running garbage collection worker")

			err := RunGarbageCollection(gc, window, timeout)
			if err != nil {
				failureCounter.Inc()
				nextInterval = backoffInterval.NextBackOff()
				log.Ctx(ctx).Warn().Err(err).
					Dur("next-attempt-in", nextInterval).
					Msg("error attempting to perform garbage collection")
				continue
			}

			backoffInterval.Reset()
			nextInterval = interval

			log.Ctx(ctx).Debug().
				Dur("next-run-in", interval).
				Msg("datastore garbage collection scheduled for next run")
		}
	}
}

// RunGarbageCollection runs garbage collection for the datastore.
func RunGarbageCollection(gc GarbageCollector, window, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "RunGarbageCollection")
	defer span.End()

	// Before attempting anything, check if the datastore is ready.
	startTime := time.Now()
	ready, err := gc.ReadyState(ctx)
	if err != nil {
		return err
	}
	if !ready.IsReady {
		log.Ctx(ctx).Warn().
			Msgf("datastore wasn't ready when attempting garbage collection: %s", ready.Message)
		return nil
	}

	ok, err := gc.LockForGCRun(ctx)
	if err != nil {
		return fmt.Errorf("error locking for gc run: %w", err)
	}

	if !ok {
		log.Info().
			Msg("datastore garbage collection already in progress on another node")
		return nil
	}

	defer func() {
		err := gc.UnlockAfterGCRun()
		if err != nil {
			log.Error().
				Err(err).
				Msg("error unlocking after gc run")
		}
	}()

	now, err := gc.Now(ctx)
	if err != nil {
		return fmt.Errorf("error retrieving now: %w", err)
	}

	watermark, err := gc.TxIDBefore(ctx, now.Add(-1*window))
	if err != nil {
		return fmt.Errorf("error retrieving watermark: %w", err)
	}

	collected, err := gc.DeleteBeforeTx(ctx, watermark)

	expiredRelationshipsCount, eerr := gc.DeleteExpiredRels(ctx)

	// even if an error happened, garbage would have been collected. This makes sure these are reflected even if the
	// worker eventually fails or times out.
	gcRelationshipsCounter.Add(float64(collected.Relationships))
	gcTransactionsCounter.Add(float64(collected.Transactions))
	gcNamespacesCounter.Add(float64(collected.Namespaces))
	gcExpiredRelationshipsCounter.Add(float64(expiredRelationshipsCount))
	collectionDuration := time.Since(startTime)
	gcDurationHistogram.Observe(collectionDuration.Seconds())

	if err != nil {
		return fmt.Errorf("error deleting in gc: %w", err)
	}

	if eerr != nil {
		return fmt.Errorf("error deleting expired relationships in gc: %w", eerr)
	}

	log.Ctx(ctx).Info().
		Stringer("highestTxID", watermark).
		Dur("duration", collectionDuration).
		Time("nowTime", now).
		Interface("collected", collected).
		Int64("expiredRelationships", expiredRelationshipsCount).
		Msg("datastore garbage collection completed successfully")

	gc.MarkGCCompleted()
	return nil
}
