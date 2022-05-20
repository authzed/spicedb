package postgres

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/datastore"
)

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
	startTime := time.Now()
	defer func() {
		gcDurationHistogram.Observe(time.Since(startTime).Seconds())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), pgd.gcMaxOperationTime)
	defer cancel()

	// Ensure the database is ready.
	ready, err := pgd.IsReady(ctx)
	if err != nil {
		return err
	}

	if !ready {
		log.Ctx(ctx).Warn().Msg("cannot perform postgres garbage collection: postgres driver is not yet ready")
		return nil
	}

	now, err := pgd.getNow(ctx)
	if err != nil {
		return err
	}

	before := now.Add(pgd.gcWindowInverted)
	log.Ctx(ctx).Debug().Time("before", before).Msg("running postgres garbage collection")
	_, _, err = pgd.collectGarbageBefore(ctx, before)
	return err
}

func (pgd *pgDatastore) collectGarbageBefore(ctx context.Context, before time.Time) (int64, int64, error) {
	// Find the highest transaction ID before the GC window.
	sql, args, err := getRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return 0, 0, err
	}

	value := pgtype.Int8{}
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&value)
	if err != nil {
		return 0, 0, err
	}

	if value.Status != pgtype.Present {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return 0, 0, nil
	}

	var highest uint64
	err = value.AssignTo(&highest)
	if err != nil {
		return 0, 0, err
	}

	log.Ctx(ctx).Trace().Uint64("highestTransactionId", highest).Msg("retrieved transaction ID for GC")

	return pgd.collectGarbageForTransaction(ctx, highest)
}

func (pgd *pgDatastore) collectGarbageForTransaction(ctx context.Context, highest uint64) (int64, int64, error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	relCount, err := pgd.batchDelete(ctx, tableTuple, sq.LtOrEq{colDeletedTxn: highest})
	if err != nil {
		return 0, 0, err
	}

	log.Ctx(ctx).Trace().Uint64("highestTransactionId", highest).Int64("relationshipsDeleted", relCount).Msg("deleted stale relationships")
	gcRelationshipsClearedGauge.Set(float64(relCount))

	// Delete all transaction rows with ID < the transaction ID. We don't delete the transaction
	// itself to ensure there is always at least one transaction present.
	transactionCount, err := pgd.batchDelete(ctx, tableTransaction, sq.Lt{colID: highest})
	if err != nil {
		return relCount, 0, err
	}

	log.Ctx(ctx).Trace().Uint64("highestTransactionId", highest).Int64("transactionsDeleted", transactionCount).Msg("deleted stale transactions")
	gcTransactionsClearedGauge.Set(float64(transactionCount))
	return relCount, transactionCount, nil
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
