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
	ReadyState(context.Context) (datastore.ReadyState, error)
	Now(context.Context) (time.Time, error)
	TxIDBefore(context.Context, time.Time) (datastore.Revision, error)
	DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (DeletionCounts, error)
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
	return startGarbageCollectorWithMaxElapsedTime(ctx, gc, interval, window, 0, timeout)
}

func startGarbageCollectorWithMaxElapsedTime(ctx context.Context, gc GarbageCollector, interval, window, maxElapsedTime, timeout time.Duration) error {
	backoffInterval := backoff.NewExponentialBackOff()
	backoffInterval.InitialInterval = interval
	backoffInterval.MaxInterval = max(MaxGCInterval, interval)
	backoffInterval.MaxElapsedTime = maxElapsedTime

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
				gcFailureCounter.Inc()
				nextInterval = backoffInterval.NextBackOff()
				log.Ctx(ctx).Warn().Err(err).
					Dur("next-attempt-in", nextInterval).
					Msg("error attempting to perform garbage collection")
				continue
			}
			backoffInterval.Reset()
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

	now, err := gc.Now(ctx)
	if err != nil {
		return fmt.Errorf("error retrieving now: %w", err)
	}

	watermark, err := gc.TxIDBefore(ctx, now.Add(-1*window))
	if err != nil {
		return fmt.Errorf("error retrieving watermark: %w", err)
	}

	collected, err := gc.DeleteBeforeTx(ctx, watermark)
	if err != nil {
		return fmt.Errorf("error deleting in gc: %w", err)
	}

	collectionDuration := time.Since(startTime)
	log.Ctx(ctx).Info().
		Stringer("highestTxID", watermark).
		Dur("duration", collectionDuration).
		Time("nowTime", now).
		Interface("collected", collected).
		Msg("datastore garbage collection completed successfully")

	gcDurationHistogram.Observe(collectionDuration.Seconds())
	gcRelationshipsCounter.Add(float64(collected.Relationships))
	gcTransactionsCounter.Add(float64(collected.Transactions))
	gcNamespacesCounter.Add(float64(collected.Namespaces))
	return nil
}
