package mysql

import (
	"context"
	"database/sql"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	gcDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_mysql",
		Name:      "gc_duration_seconds",
		Help:      "The duration of MySQL datastore garbage collection.",
		Buckets:   []float64{0.01, 0.1, 0.5, 1, 5, 10, 25, 60, 120},
	})

	gcRelationshipsClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_mysql",
		Name:      "gc_relationships_total",
		Help:      "The number of relationships cleared by the MySQL datastore garbage collection.",
	})

	gcTransactionsClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_mysql",
		Name:      "gc_transactions_total",
		Help:      "The number of transactions cleared by the MySQL datastore garbage collection.",
	})

	gcNamespacesClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_mysql",
		Name:      "gc_namespaces_total",
		Help:      "The number of namespaces cleared by the MySQL datastore garbage collection.",
	})
)

func registerGCMetrics() error {
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

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) runGarbageCollector() error {
	log.Info().Dur("interval", mds.gcInterval).Msg("garbage collection worker started for mysql driver")

	for {
		select {
		case <-mds.gcCtx.Done():
			log.Info().Msg("shutting down garbage collection worker for mysql driver")
			return mds.gcCtx.Err()

		case <-time.After(mds.gcInterval):
			err := mds.collectGarbage()
			if err != nil {
				log.Warn().Err(err).Msg("error when attempting to perform garbage collection")
			}
		}
	}
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - an additional useful logging message is added
// - context is removed from logger because zerolog expects logger in the context
func (mds *Datastore) collectGarbage() error {
	ctx, cancel := context.WithTimeout(context.Background(), mds.gcMaxOperationTime)
	defer cancel()

	var (
		startTime = time.Now()
		amounts   garbageAmounts
		watermark uint64
	)

	defer func() {
		collectionDuration := time.Since(startTime)
		gcDurationHistogram.Observe(collectionDuration.Seconds())

		log.Ctx(ctx).Trace().
			Uint64("highestTxID", watermark).
			Dur("duration", collectionDuration).
			Interface("collected", amounts).
			Msg("datastore garbage collection completed")

		gcRelationshipsClearedCounter.Add(float64(amounts.relationships))
		gcTransactionsClearedCounter.Add(float64(amounts.transactions))
		gcNamespacesClearedCounter.Add(float64(amounts.namespaces))
	}()

	// Ensure the database is ready.
	ready, err := mds.IsReady(ctx)
	if err != nil {
		return err
	}

	if !ready {
		log.Ctx(ctx).Warn().Msg("cannot perform datastore garbage collection: datastore is not yet ready")
		return nil
	}

	now, err := mds.getNow(ctx)
	if err != nil {
		return err
	}

	watermark, err = mds.mostRecentTxBefore(ctx, now.Add(mds.gcWindowInverted))
	if err != nil {
		return err
	}

	amounts, err = mds.deleteBefore(ctx, watermark)
	return err
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - main difference is how the PSQL driver handles null values
func (mds *Datastore) mostRecentTxBefore(ctx context.Context, before time.Time) (uint64, error) {
	// Find the highest transaction ID before the GC window.
	query, args, err := mds.GetLastRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return 0, err
	}

	var value sql.NullInt64
	err = mds.db.QueryRowContext(
		datastore.SeparateContextWithTracing(ctx), query, args...,
	).Scan(&value)
	if err != nil {
		return 0, err
	}

	if !value.Valid {
		log.Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return 0, nil
	}
	return uint64(value.Int64), nil
}

type garbageAmounts struct {
	relationships int64
	transactions  int64
	namespaces    int64
}

func (g garbageAmounts) MarshalZerologObject(e *zerolog.Event) {
	e.
		Int64("relationships", g.relationships).
		Int64("transactions", g.transactions).
		Int64("namespaces", g.namespaces)
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - implementation misses metrics
func (mds *Datastore) deleteBefore(ctx context.Context, txID uint64) (amounts garbageAmounts, err error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	amounts.relationships, err = mds.batchDelete(ctx, mds.driver.RelationTuple(), sq.LtOrEq{colDeletedTxn: txID})
	if err != nil {
		return
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	amounts.transactions, err = mds.batchDelete(ctx, mds.driver.RelationTupleTransaction(), sq.Lt{colID: txID})
	if err != nil {
		return
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	amounts.namespaces, err = mds.batchDelete(ctx, mds.driver.Namespace(), sq.Lt{colID: txID})
	return
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - query was reworked to make it compatible with Vitess
// - API differences with PSQL driver
func (mds *Datastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	query, args, err := sb.Delete(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	var deletedCount int64
	for {
		cr, err := mds.db.ExecContext(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted, err := cr.RowsAffected()
		if err != nil {
			return deletedCount, err
		}
		deletedCount += rowsDeleted
		if rowsDeleted < batchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}
