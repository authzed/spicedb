package postgres

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	gcDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "gc_duration_seconds",
		Help:      "The duration of Postgres datastore garbage collection.",
		Buckets:   []float64{0.01, 0.1, 0.5, 1, 5, 10, 25, 60, 120},
	})

	gcRelationshipsClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "gc_relationships_total",
		Help:      "The number of relationships cleared by the Postgres datastore garbage collection.",
	})

	gcTransactionsClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "gc_transactions_total",
		Help:      "The number of transactions cleared by the Postgres datastore garbage collection.",
	})

	gcNamespacesClearedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "gc_namespaces_total",
		Help:      "The number of namespaces cleared by the Postgres datastore garbage collection.",
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

func (pgd *pgDatastore) runGarbageCollector() error {
	log.Info().Dur("interval", pgd.gcInterval).Msg("garbage collection worker started for postgres driver")

	for {
		select {
		case <-pgd.gcCtx.Done():
			log.Info().Msg("shutting down garbage collection worker for postgres driver")
			return pgd.gcCtx.Err()

		case <-time.After(pgd.gcInterval):
			err := pgd.collectGarbage()
			if err != nil {
				log.Warn().Err(err).Msg("error when attempting to perform garbage collection")
			} else {
				log.Debug().Msg("garbage collection completed for postgres")
			}
		}
	}
}

func (pgd *pgDatastore) collectGarbage() error {
	ctx, cancel := context.WithTimeout(context.Background(), pgd.gcMaxOperationTime)
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
	ready, err := pgd.IsReady(ctx)
	if err != nil {
		return err
	}

	if !ready {
		log.Ctx(ctx).Warn().Msg("cannot perform datastore garbage collection: datastore is not yet ready")
		return nil
	}

	now, err := pgd.getNow(ctx)
	if err != nil {
		return err
	}

	watermark, err = pgd.mostRecentTxBefore(ctx, now.Add(pgd.gcWindowInverted))
	if err != nil {
		return err
	}

	amounts, err = pgd.deleteBefore(ctx, watermark)
	return err
}

func (pgd *pgDatastore) mostRecentTxBefore(ctx context.Context, before time.Time) (uint64, error) {
	// Find the highest transaction ID before the GC window.
	sql, args, err := getRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return 0, err
	}

	value := pgtype.Int8{}
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&value)
	if err != nil {
		return 0, err
	}

	if value.Status != pgtype.Present {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return 0, err
	}

	var highest uint64
	err = value.AssignTo(&highest)
	if err != nil {
		return 0, err
	}

	return highest, nil
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

func (pgd *pgDatastore) deleteBefore(ctx context.Context, txID uint64) (amounts garbageAmounts, err error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	amounts.relationships, err = pgd.batchDelete(ctx, tableTuple, sq.LtOrEq{colDeletedTxn: txID})
	if err != nil {
		return
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	amounts.transactions, err = pgd.batchDelete(ctx, tableTransaction, sq.Lt{colID: txID})
	if err != nil {
		return
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	amounts.namespaces, err = pgd.batchDelete(ctx, tableNamespace, sq.Lt{colID: txID})
	if err != nil {
		return
	}

	return
}

func (pgd *pgDatastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	sql, args, err := psql.Select("id").From(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	query := fmt.Sprintf(`WITH rows AS (%s)
		  DELETE FROM %s
		  WHERE id IN (SELECT id FROM rows);
	`, sql, tableName)

	var deletedCount int64
	for {
		cr, err := pgd.dbpool.Exec(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted := cr.RowsAffected()
		deletedCount += rowsDeleted
		if rowsDeleted < batchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}
