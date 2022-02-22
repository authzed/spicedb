package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	"github.com/go-co-op/gocron"
	"github.com/rs/zerolog/log"
)

// runGC runs the gc cron job, returning an error if it can't start properly.
// it cleans up and stops when ctx is Done.
func (sd spannerDatastore) runGC(ctx context.Context) error {
	if sd.config.gcInterval <= 0 {
		log.Info().Stringer("interval", sd.config.gcInterval).Msg("garbage collection: disabled")
		return nil
	}

	log.Info().Stringer("interval", sd.config.gcInterval).Msg("garbage collection: starting")

	s := gocron.NewScheduler(time.UTC)

	var numRemoved int64
	_, err := s.Every(sd.config.gcInterval).Do(func() {
		ctx, span := tracer.Start(context.Background(), "CollectGarbage")
		defer span.End()

		ctx, cancel := context.WithTimeout(ctx, sd.config.gcInterval)
		defer cancel()

		spannerNow, err := sd.now(ctx)
		if err != nil {
			log.Error().Err(err).Msg("garbage collection: error computing datastore time")
		}

		oldestRevision := spannerNow.Add(-1 * sd.config.gcWindow)

		stmt, args, err := sql.Delete(tableChangelog).Where(sq.Lt{colChangeTS: oldestRevision}).ToSql()
		if err != nil {
			log.Error().Err(err).Msg("garbage collection: error creating delete statement")
		}

		_, err = sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
			numRemoved, err = rwt.Update(ctx, statementFromSQL(stmt, args))
			return err
		})
		if err != nil {
			log.Error().Err(err).Msg("garbage collection: error deleting entries")
		}

		log.Info().Int64("removed", numRemoved).Stringer("before", oldestRevision).
			Msg("garbage collection: removed changelog entries")
	})
	if err != nil {
		return fmt.Errorf("unable to start garbage collection: %w", err)
	}

	go func() {
		<-ctx.Done()
		log.Info().Msg("garbage collection: stopping")
		s.Stop()
	}()
	return nil
}
