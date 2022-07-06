package common

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
)

// RegisterGCMetrics registers garbage collection metrics to the default
// registry.
func RegisterGCMetrics() error {
	for _, metric := range []prometheus.Collector{
		gcDurationHistogram,
		gcRelationshipsCounter,
		gcTransactionsCounter,
		gcNamespacesCounter,
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
	IsReady(context.Context) (bool, error)
	Now(context.Context) (time.Time, error)
	TxIDBefore(context.Context, time.Time) (uint64, error)
	DeleteBeforeTx(ctx context.Context, txID uint64) (DeletionCounts, error)
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

// StartGarbageCollector loops forever until the context is canceled and
// performs garbage collection on the provided interval.
func StartGarbageCollector(ctx context.Context, gc GarbageCollector, interval, window, timeout time.Duration) error {
	log.Ctx(ctx).Info().
		Dur("interval", interval).
		Msg("datastore garbage collection worker started")

	for {
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Info().
				Msg("shutting down datastore garbage collection worker")
			return ctx.Err()

		case <-time.After(interval):
			err := collect(gc, window, timeout)
			if err != nil {
				log.Ctx(ctx).Warn().Err(err).
					Msg("error attempting to perform garbage collection")
			}
		}
	}
}

func collect(gc GarbageCollector, window, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Before attempting anything, check if the datastore is ready.
	ready, err := gc.IsReady(ctx)
	if err != nil {
		return err
	}
	if !ready {
		log.Ctx(ctx).Warn().
			Msg("datastore wasn't ready when attempting garbage collection")
		return nil
	}

	var (
		startTime = time.Now()
		collected DeletionCounts
		watermark uint64
	)

	defer func() {
		collectionDuration := time.Since(startTime)
		gcDurationHistogram.Observe(collectionDuration.Seconds())

		log.Ctx(ctx).Debug().
			Uint64("highestTxID", watermark).
			Dur("duration", collectionDuration).
			Interface("collected", collected).
			Msg("datastore garbage collection completed")

		gcRelationshipsCounter.Add(float64(collected.Relationships))
		gcTransactionsCounter.Add(float64(collected.Transactions))
		gcNamespacesCounter.Add(float64(collected.Namespaces))
	}()

	now, err := gc.Now(ctx)
	if err != nil {
		return err
	}

	watermark, err = gc.TxIDBefore(ctx, now.Add(-1*window))
	if err != nil {
		return err
	}

	collected, err = gc.DeleteBeforeTx(ctx, watermark)
	return err
}
