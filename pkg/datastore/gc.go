package datastore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	log "github.com/authzed/spicedb/internal/logging"
)

var gcTracer = otel.Tracer("spicedb/pkg/datastore")

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

// ErrGCPreempted is returned when a garbage collection run is preempted by
// another node acquiring the GC lock. This is not an error condition — it
// means another node took over GC and this node should stop.
var ErrGCPreempted = errors.New("gc preempted by another node")

// RegisterGCMetrics registers garbage collection metrics to the default
// registry and returns them (so that they be unregistered).
func RegisterGCMetrics() ([]prometheus.Collector, error) {
	collectors := []prometheus.Collector{
		gcDurationHistogram,
		gcRelationshipsCounter,
		gcTransactionsCounter,
		gcNamespacesCounter,
		gcFailureCounter,
	}
	for _, metric := range collectors {
		if err := prometheus.Register(metric); err != nil {
			return nil, fmt.Errorf("failed to register GC metric: %w", err)
		}
	}

	return collectors, nil
}

// GarbageCollectableDatastore represents a datastore supporting external
// and explicit garbage collection.
type GarbageCollectableDatastore interface {
	// BuildGarbageCollector builds a garbage collector for the datastore.
	//
	// Each instance is considered isolated and will be used for the duration
	// of a *single* garbage collection call.
	BuildGarbageCollector(ctx context.Context) (GarbageCollector, error)

	// ReadyState returns the current state of the datastore. Note that this does not
	// operate over the dedicated connection.
	ReadyState(context.Context) (ReadyState, error)

	// HasGCRun returns true if a garbage collection run has been completed.
	HasGCRun() bool

	// MarkGCCompleted marks that a garbage collection run has been completed.
	MarkGCCompleted()

	// ResetGCCompleted resets the state of the garbage collection run.
	ResetGCCompleted()
}

// GarbageCollector represents a runnable garbage collector for a datastore.
type GarbageCollector interface {
	// Now returns the current time from the datastore.
	Now(context.Context) (time.Time, error)

	// TxIDBefore returns the highest transaction ID before the provided time.
	TxIDBefore(context.Context, time.Time) (Revision, error)

	// DeleteBeforeTx deletes all data before the provided transaction ID.
	// Returns ErrGCPreempted if another node acquired the GC lock.
	DeleteBeforeTx(ctx context.Context, txID Revision) (DeletionCounts, error)

	// DeleteExpiredRels deletes all relationships that have expired.
	// Returns ErrGCPreempted if another node acquired the GC lock.
	DeleteExpiredRels(ctx context.Context) (int64, error)

	// Close closes the garbage collector.
	Close()
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

// MaxGCInterval is the maximum interval between GC runs.
var MaxGCInterval = 60 * time.Minute

// StartGarbageCollector loops forever until the context is canceled and
// performs garbage collection on the provided interval.
func StartGarbageCollector(ctx context.Context, collectable GarbageCollectableDatastore, interval, window, timeout time.Duration) error {
	return runGCOnIntervalWithBackoff(ctx, func() error {
		// NOTE: we're okay using the parent context here because the
		// callers of this function create a dedicated garbage collection
		// context anyway, which is only cancelled when the ds is closed.
		gcCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return RunGarbageCollection(gcCtx, collectable, window)
	}, interval, timeout, gcFailureCounter)
}

func runGCOnIntervalWithBackoff(ctx context.Context, taskFn func() error, interval, timeout time.Duration, failureCounter prometheus.Counter) error {
	backoffInterval := backoff.NewExponentialBackOff()
	backoffInterval.InitialInterval = interval
	backoffInterval.MaxInterval = max(MaxGCInterval, interval)
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
				Dur("timeout", timeout).
				Msg("running garbage collection worker")

			err := taskFn()
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
func RunGarbageCollection(ctx context.Context, collectable GarbageCollectableDatastore, window time.Duration) error {
	ctx, span := gcTracer.Start(ctx, "RunGarbageCollection")
	defer span.End()

	// Before attempting anything, check if the datastore is ready.
	startTime := time.Now()
	ready, err := collectable.ReadyState(ctx)
	if err != nil {
		return err
	}
	if !ready.IsReady {
		log.Ctx(ctx).Warn().
			Msgf("datastore wasn't ready when attempting garbage collection: %s", ready.Message)
		return nil
	}

	gc, err := collectable.BuildGarbageCollector(ctx)
	if err != nil {
		return fmt.Errorf("error building garbage collector: %w", err)
	}
	defer gc.Close()

	now, err := gc.Now(ctx)
	if err != nil {
		return fmt.Errorf("error retrieving now: %w", err)
	}

	watermark, err := gc.TxIDBefore(ctx, now.Add(-1*window))
	if err != nil {
		return fmt.Errorf("error retrieving watermark: %w", err)
	}

	collected, err := gc.DeleteBeforeTx(ctx, watermark)

	var expiredRelationshipsCount int64
	var eerr error
	if !errors.Is(err, ErrGCPreempted) {
		expiredRelationshipsCount, eerr = gc.DeleteExpiredRels(ctx)
	}

	// even if an error happened, garbage would have been collected. This makes sure these are reflected even if the
	// worker eventually fails or times out.
	gcRelationshipsCounter.Add(float64(collected.Relationships))
	gcTransactionsCounter.Add(float64(collected.Transactions))
	gcNamespacesCounter.Add(float64(collected.Namespaces))
	gcExpiredRelationshipsCounter.Add(float64(expiredRelationshipsCount))
	collectionDuration := time.Since(startTime)
	gcDurationHistogram.Observe(collectionDuration.Seconds())

	if errors.Is(err, ErrGCPreempted) || errors.Is(eerr, ErrGCPreempted) {
		log.Ctx(ctx).Info().
			Stringer("highestTxID", watermark).
			Dur("duration", collectionDuration).
			Interface("collected", collected).
			Int64("expiredRelationships", expiredRelationshipsCount).
			Msg("datastore garbage collection partially completed: preempted by another node")
		// Still mark as completed — partial GC is valid, and the other node
		// will continue the work.
		collectable.MarkGCCompleted()
		return nil
	}

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

	collectable.MarkGCCompleted()
	return nil
}
