package datastore

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// BulkDeleteOptions configures BulkDeleteRelationships.
type BulkDeleteOptions struct {
	// BatchSize is the maximum number of relationships deleted per
	// transaction. Required; must be positive.
	BatchSize uint64

	// SleepBetweenBatches is paused between batches, to bound the load placed
	// on the datastore. Zero means no pause.
	SleepBetweenBatches time.Duration

	// ResumeCursor, if set, starts the first batch after this relationship
	// rather than at the beginning. Ignored on the fallback path, which has no
	// cursor.
	ResumeCursor options.Cursor

	// OnBatch, if set, is called after each committed batch.
	OnBatch func(BulkDeleteProgress)
}

// BulkDeleteProgress reports the state of a bulk deletion.
type BulkDeleteProgress struct {
	// Pass is the 1-based sweep number. See BulkDeleteRelationships for why
	// there can be more than one.
	Pass uint64

	// Batches is the number of committed transactions so far, across passes.
	Batches uint64

	// TotalDeleted is the number of relationships deleted so far, across passes.
	TotalDeleted uint64

	// LastBatchDeleted is the number deleted by the most recent batch.
	LastBatchDeleted uint64

	// Cursor is the resume point after the most recent batch, or nil on the
	// fallback path.
	Cursor options.Cursor

	// Cursored reports whether the cursored path or the fallback is in use.
	Cursored bool
}

// BulkDeleteRelationships deletes every relationship matching the filter, in
// batches, committing each batch separately.
//
// On a datastore implementing CursoredDeleteDatastore, each batch resumes after
// the previous batch's cursor, so a batch never rescans the tombstones left by
// its predecessors. Because rows inserted during the deletion whose keys sort
// before the current cursor are never visited, the cursored path repeats passes
// from the beginning until a full pass deletes nothing. Under writes that
// continuously match the filter this does not converge; pause writes to the
// filter for a complete deletion.
//
// On every other datastore it falls back to limited deletes with no cursor,
// which is correct but rescans from the start of the range on every batch.
//
// The loop is never wrapped in a single transaction: one transaction holding a
// very large delete is the failure mode this exists to avoid.
func BulkDeleteRelationships(
	ctx context.Context,
	ds Datastore,
	filter *v1.RelationshipFilter,
	opts BulkDeleteOptions,
) (BulkDeleteProgress, error) {
	if opts.BatchSize == 0 {
		return BulkDeleteProgress{}, errors.New("bulk delete requires a positive batch size")
	}

	cursored := false
	if cds := UnwrapAs[CursoredDeleteDatastore](ds); cds != nil && cds.SupportsCursoredDelete() {
		cursored = true
	} else {
		log.Ctx(ctx).Warn().Msg(
			"datastore does not support cursored deletion; falling back to the slower limited-loop path")
		if opts.ResumeCursor != nil {
			log.Ctx(ctx).Warn().Msg(
				"--resume-cursor was provided but this datastore does not support cursored deletion; it is being ignored and the deletion will restart from the beginning of the filter's range")
		}
	}

	progress := BulkDeleteProgress{Cursored: cursored}
	cursor := opts.ResumeCursor

	for {
		progress.Pass++
		// A pass that starts after a non-nil cursor -- the first pass of a run
		// resumed with --resume-cursor, or any pass whose predecessor advanced
		// the cursor -- never visits the range before its starting point.
		// Recorded before the batch loop below overwrites cursor with wherever
		// this pass leaves off.
		startedAfterCursor := cursor != nil
		deletedThisPass := uint64(0)

		for {
			if err := ctx.Err(); err != nil {
				return progress, err
			}

			batchSize := opts.BatchSize
			delOpts := []options.DeleteOptionsOption{options.WithDeleteLimit(&batchSize)}
			if cursored {
				delOpts = append(delOpts, options.WithCursoredDelete(true))
				if cursor != nil {
					delOpts = append(delOpts, options.WithDeleteAfter(cursor))
				}
			}

			// The closure captures the batch's starting cursor and never
			// mutates it, so a serialization retry re-runs against the same
			// start and cannot skip a range of rows. The result is published
			// only after the transaction commits.
			var result DeleteRelationshipsResult
			if _, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				var innerErr error
				result, innerErr = rwt.DeleteRelationships(ctx, filter, delOpts...)
				return innerErr
			}, options.WithSkipCommitRevision(true)); err != nil {
				return progress, fmt.Errorf("bulk delete batch failed: %w", err)
			}

			if result.NumDeleted == 0 {
				break
			}

			progress.Batches++
			progress.TotalDeleted += result.NumDeleted
			progress.LastBatchDeleted = result.NumDeleted
			deletedThisPass += result.NumDeleted

			if cursored {
				if result.Cursor == nil {
					return progress, errors.New(
						"datastore reported a cursored delete without a cursor; cannot continue safely")
				}
				cursor = result.Cursor
				progress.Cursor = result.Cursor
			}

			if opts.OnBatch != nil {
				opts.OnBatch(progress)
			}

			// The fallback path restarts from the top of the range on every
			// batch, so a batch that did not fill the limit means the filter is
			// exhausted.
			if !cursored && !result.LimitReached {
				break
			}

			if opts.SleepBetweenBatches > 0 {
				select {
				case <-ctx.Done():
					return progress, ctx.Err()
				case <-time.After(opts.SleepBetweenBatches):
				}
			}
		}

		// The fallback path needs no second sweep: every batch already starts at
		// the top of the range.
		if !cursored {
			return progress, nil
		}

		// A pass that both started at the top of the range and deleted nothing
		// is genuinely complete: there is nothing left, and nothing was skipped.
		// A pass that started after a cursor -- most notably a run's first pass
		// after --resume-cursor -- must not be trusted the same way even when it
		// deletes nothing itself: the range before its starting point was never
		// visited, so at least one more pass beginning at nil is required to
		// check it.
		if deletedThisPass == 0 && !startedAfterCursor {
			return progress, nil
		}

		// Restart the sweep to catch rows inserted behind the cursor.
		cursor = nil
		progress.Cursor = nil
	}
}
