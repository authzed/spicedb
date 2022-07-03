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

	gcRelationshipsClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_relationships_total",
		Help:      "The number of relationships cleared by the datastore garbage collection.",
	})

	gcTransactionsClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_transactions_total",
		Help:      "The number of transactions cleared by the datastore garbage collection.",
	})

	gcNamespacesClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "gc_namespaces_total",
		Help:      "The number of namespaces cleared by the datastore garbage collection.",
	})
)

// RegisterGCMetrics registers garbage collection metrics to the default
// registry.
func RegisterGCMetrics() error {
	for _, metric := range []prometheus.Collector{
		gcDurationHistogram,
		gcRelationshipsClearedCounter,
		gcTransactionsClearedCounter,
		gcNamespacesClearedCounter,
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
	DeleteBeforeTx(context.Context, uint64) (GarbageCollectedAmounts, error)
}

// GarbageCollectedAmounts is the collection of amounts of deletions from
// an individual run of the garbage collector.
type GarbageCollectedAmounts struct {
	Relationships int64
	Transactions  int64
	Namespaces    int64
}

func (g GarbageCollectedAmounts) MarshalZerologObject(e *zerolog.Event) {
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
		amounts   GarbageCollectedAmounts
		watermark uint64
	)

	defer func() {
		collectionDuration := time.Since(startTime)
		gcDurationHistogram.Observe(collectionDuration.Seconds())

		log.Ctx(ctx).Debug().
			Uint64("highestTxID", watermark).
			Dur("duration", collectionDuration).
			Interface("collected", amounts).
			Msg("datastore garbage collection completed")

		gcRelationshipsClearedCounter.Add(float64(amounts.Relationships))
		gcTransactionsClearedCounter.Add(float64(amounts.Transactions))
		gcNamespacesClearedCounter.Add(float64(amounts.Namespaces))
	}()

	now, err := gc.Now(ctx)
	if err != nil {
		return err
	}

	watermark, err = gc.TxIDBefore(ctx, now.Add(-1*window))
	if err != nil {
		return err
	}

	amounts, err = gc.DeleteBeforeTx(ctx, watermark)
	return err
}
