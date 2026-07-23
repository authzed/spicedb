package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// The cursor watch is an alternative implementation of the datastore Watch API
// for PostgreSQL that delivers transactions in true commit order, keyed by the
// commit positions the commit LSN ledger (see lsn_ledger.go) records into
// relation_tuple_transaction.commit_lsn.
//
// It is the polling watch with a different discovery query. Where the polling
// watch tracks a snapshot and orders by pg_xact_commit_timestamp, the cursor
// watch tracks a single pg_lsn cursor and asks for
//
//	commit_lsn > cursor AND commit_lsn <= frontier, ORDER BY commit_lsn
//
// where the frontier is the ledger slot's confirmed_flush_lsn. The frontier is
// a completeness bound: the ledger decodes WAL in order and confirms a position
// only after everything at or below it is durably recorded, so every
// transaction with commit_lsn <= frontier is present in the table. Advancing
// the cursor to the last delivered position therefore yields every
// watch-visible transaction exactly once, in commit order, with no gap and no
// duplicate. There is no second delivery path to agree with: the ledger is the
// only WAL decoder, and every token is a pure function of the transaction it
// names.
//
// What a consumer sees:
//
//   - A revision's string form is the 16-hex big-endian commit LSN, a '.', and
//     the base64 revision proto. The prefix is fixed width, so byte order is
//     commit order, and comparing two tokens from different Watch calls, or
//     from different consumers, is meaningful. Commit-LSN order is a linear
//     extension of the MVCC snapshot partial order, so token comparison never
//     contradicts snapshot-based ground truth.
//   - Change events arrive strictly ascending by commit LSN, with no seam.
//   - A checkpoint means delivery is complete through its position. Checkpoints
//     are strictly monotone and exact: resuming from one neither loses nor
//     repeats events. Change-event tokens are equally exact resume points.
//   - Resuming from a snapshot-only revision (a polling-watch token, or a
//     HeadRevision predating the feature) is served by a one-time backfill
//     phase before the cursor loop takes over; see beginCursorWatch.
//   - Transactions that committed before the ledger was provisioned have no
//     recoverable position and are delivered first, in commit order, as
//     unpositioned revisions: the same token shape the polling watch emits.
//     Those tokens must not be byte-compared with positioned ones.
//   - A transaction lost to a ledger slot recreation is recorded as a gap
//     (see ledger_gap); a watch positioned below a gap fails with an explicit
//     error instead of stepping over the missing transactions, and its
//     consumer must restart from a current revision.
//   - A cursor below the retained history fails with RevisionStale instead of
//     silently truncating the stream.
const (
	// ledgerFrontierProbeInterval is how often a starting Watch call re-reads its
	// marker transaction's recorded commit position while waiting for the commit
	// LSN ledger to reach it.
	ledgerFrontierProbeInterval = 20 * time.Millisecond

	// minimumCursorWatchPollInterval floors the discovery poll interval so a
	// misconfigured value cannot spin the database.
	minimumCursorWatchPollInterval = 5 * time.Millisecond
)

var (
	// errCursorWatchDisconnected indicates that the `send` callback timed out
	// writing to the `updates` channel. The callback itself has already
	// delivered the disconnection error on the error channel.
	errCursorWatchDisconnected = errors.New("cursor watch disconnected")

	// cursorWatchRevisionsQuery is the discovery query: every transaction whose
	// recorded commit position lies in (cursor, frontier], in commit order. The
	// interval's lower end is exclusive and its upper end inclusive, and the
	// frontier bound is what makes an empty result mean "nothing committed"
	// rather than "nothing recorded yet".
	//
	// It drives from the position table, whose index over (commit_lsn) INCLUDE
	// (xid) answers the range scan without touching that table's heap, and then
	// looks each transaction up by primary key. A position whose transaction has
	// been garbage collected joins away, which is correct: it sits below every
	// live consumer's cursor.
	cursorWatchRevisionsQuery = fmt.Sprintf(`
	SELECT t.%[1]s, t.%[2]s, t.%[3]s, t.%[4]s, p.%[6]s::text
	FROM %[7]s p
	JOIN %[5]s t ON t.%[1]s = p.%[1]s
	WHERE p.%[6]s > $1::pg_lsn AND p.%[6]s <= $2::pg_lsn
	ORDER BY p.%[6]s
	LIMIT $3;`,
		schema.ColXID, schema.ColSnapshot, schema.ColMetadata, schema.ColTimestamp,
		schema.TableTransaction, schema.ColCommitLSN, schema.TableLedgerXidLSN)

	// catchupRevisionsQuery returns the transactions that are visible in the
	// snapshot given as $2 but not in the snapshot given as $1, in commit order.
	// It serves the backfill phase for a caller resuming from a snapshot-only
	// revision.
	//
	// The join is an outer one because a transaction with no recorded position
	// is meaningful here: it committed before the ledger existed, so it really
	// did precede everything recorded, and NULLS FIRST puts it there. Ties among
	// those are broken by the transaction row's timestamp rather than
	// pg_xact_commit_timestamp, so the cursor watch does not require
	// track_commit_timestamp=on. checkUnrecordedCatchupRevisions decides whether
	// each such transaction is genuinely pre-ledger or a gap victim.
	catchupRevisionsQuery = fmt.Sprintf(`
	SELECT t.%[1]s, t.%[2]s, t.%[3]s, t.%[4]s, p.%[6]s::text
	FROM %[5]s t
	LEFT JOIN %[7]s p ON p.%[1]s = t.%[1]s
	WHERE pg_visible_in_snapshot(t.%[1]s, $2) AND (
		t.%[1]s >= pg_snapshot_xmax($1) OR (
			t.%[1]s >= pg_snapshot_xmin($1) AND NOT pg_visible_in_snapshot(t.%[1]s, $1)
		)
	) ORDER BY p.%[6]s ASC NULLS FIRST, t.%[4]s, t.%[1]s;`,
		schema.ColXID, schema.ColSnapshot, schema.ColMetadata, schema.ColTimestamp,
		schema.TableTransaction, schema.ColCommitLSN, schema.TableLedgerXidLSN)

	// insertWatchMarkerQuery writes the marker transaction whose recorded commit
	// position proves the commit LSN ledger has caught up past everything the
	// backfill phase will replay. Like the revision heartbeat, the marker is a
	// bare transaction row: it carries no metadata and produces no watch-visible
	// changes for any other watcher.
	insertWatchMarkerQuery = fmt.Sprintf(
		`INSERT INTO %[1]s (%[2]s, %[3]s) VALUES (pg_current_xact_id(), pg_current_snapshot()) RETURNING %[2]s;`,
		schema.TableTransaction, schema.ColXID, schema.ColSnapshot,
	)

	// maxVisibleRecordedCommitLSNQuery reports the highest recorded commit
	// position among transactions visible in the given snapshot, which is where
	// the backfill phase hands off to the cursor loop: visibility in a snapshot
	// partitions transactions by commit time, and commit time order is commit
	// LSN order, so everything invisible in the snapshot lies strictly above it.
	//
	// The position table carries the xid, so the visibility test needs no join
	// to the transaction row.
	maxVisibleRecordedCommitLSNQuery = fmt.Sprintf(`
	SELECT %[1]s::text FROM %[2]s
	WHERE pg_visible_in_snapshot(%[3]s, $1)
	ORDER BY %[1]s DESC LIMIT 1;`,
		schema.ColCommitLSN, schema.TableLedgerXidLSN, schema.ColXID)
)

var (
	watchFrontierLagGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "watch_frontier_lag_bytes",
		Help:      "WAL bytes between the ledger's frontier and the most recently observed cursor watch position.",
	})

	watchPollDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "watch_poll_duration_seconds",
		Help:      "The latency of the cursor watch's discovery query.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	})

	watchBatchTransactionsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "watch_batch_transactions",
		Help:      "Transactions returned per cursor watch poll; saturation at the batch size means a backlog is draining.",
		Buckets:   []float64{1, 4, 16, 64, 256, 1024, 4096},
	})

	watchStaleRevisionCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "watch_stale_revision_total",
		Help:      "Watches rejected because the revision they resumed from is older than the garbage collection window.",
	})

	watchGapRejectionsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "watch_gap_rejections_total",
		Help:      "Watches rejected because a recorded ledger gap lies above their cursor.",
	})
)

// prepareCursorWatch validates the preconditions for the cursor watch and
// provisions the commit LSN ledger it reads from. It is invoked once at
// datastore construction, only when the watch is enabled, and fails fast when
// the server cannot support logical replication (which the ledger needs).
func (pgd *pgDatastore) prepareCursorWatch(ctx context.Context) error {
	var walLevel string
	if err := pgd.writePool.QueryRow(ctx, "SHOW wal_level;").Scan(&walLevel); err != nil {
		return fmt.Errorf("unable to determine wal_level: %w", err)
	}

	if walLevel != "logical" {
		return fmt.Errorf("the commit LSN ledger requires wal_level=logical, but the connected PostgreSQL server reports wal_level=%s", walLevel)
	}

	// Checked here rather than left to disable the watch, because the ledger's
	// slot would otherwise be provisioned and retain WAL, for a feature that
	// cannot serve. Both settings need a restart, so an operator enabling this
	// watch is already restarting and can set them together.
	var trackCommitTimestamp string
	if err := pgd.writePool.QueryRow(ctx, "SHOW track_commit_timestamp;").Scan(&trackCommitTimestamp); err != nil {
		return fmt.Errorf("unable to determine track_commit_timestamp: %w", err)
	}

	if trackCommitTimestamp != "on" {
		return fmt.Errorf("the cursor watch requires track_commit_timestamp=on, but the connected PostgreSQL server reports track_commit_timestamp=%s; a gap the ledger could not record is replayed in commit timestamp order, which is the only commit order left once the WAL holding it is gone", trackCommitTimestamp)
	}

	// Best-effort check of the REPLICATION privilege. Some managed providers
	// (e.g., RDS) grant replication through role membership rather than the
	// rolreplication attribute, so a negative result is only a warning; the
	// ledger's replication connection will fail with a clear error if the
	// privilege is truly missing.
	var canReplicate bool
	if err := pgd.writePool.QueryRow(
		ctx,
		"SELECT COALESCE((SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user), false);",
	).Scan(&canReplicate); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("unable to verify REPLICATION privilege for the commit LSN ledger")
	} else if !canReplicate {
		log.Ctx(ctx).Warn().Msg("the connected PostgreSQL user does not appear to have the REPLICATION privilege; the commit LSN ledger may fail to connect")
	}

	pgd.noteLegacyStreamingWatchLeftovers(ctx)

	return pgd.prepareCommitLSNLedger(ctx)
}

// noteLegacyStreamingWatchLeftovers reports database objects an earlier,
// streaming revision of this feature created and the cursor watch no longer
// uses. Neither is reverted automatically: a replica identity or publication
// SpiceDB set is indistinguishable from one an operator set for their own
// replication consumers, and both are harmless when unused.
func (pgd *pgDatastore) noteLegacyStreamingWatchLeftovers(ctx context.Context) {
	for _, table := range []string{schema.TableTuple, schema.TableNamespace, schema.TableCaveat} {
		var replicaIdentity string
		if err := pgd.writePool.QueryRow(
			ctx,
			"SELECT relreplident::text FROM pg_class WHERE oid = $1::regclass;", table,
		).Scan(&replicaIdentity); err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("table", table).Msg("unable to inspect replica identity")
			continue
		}
		if replicaIdentity == "f" {
			log.Ctx(ctx).Info().Str("table", table).
				Msgf("table has REPLICA IDENTITY FULL, which the watch no longer needs and which logs whole old tuples on every soft delete; if it was set for SpiceDB, revert it: ALTER TABLE %s REPLICA IDENTITY DEFAULT;", table)
		}
	}

	// The streaming watch's default publication name; a customized name cannot
	// be recovered here, so only the default is checked.
	const legacyWatchPublication = "spicedb_watch"
	var exists bool
	if err := pgd.writePool.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1);", legacyWatchPublication,
	).Scan(&exists); err == nil && exists {
		log.Ctx(ctx).Info().Str("publication", legacyWatchPublication).
			Msgf("publication exists but the watch no longer uses one; if nothing else consumes it, drop it: DROP PUBLICATION %s;", legacyWatchPublication)
	}
}

// cursorWatch implements the Watch contract on top of the commit LSN ledger.
// The updates and errs channels are created by the caller (Watch) and are owned
// by this function from this point on.
func (pgd *pgDatastore) cursorWatch(
	ctx context.Context,
	afterRevisionRaw datastore.Revision,
	options datastore.WatchOptions,
	updates chan datastore.RevisionChanges,
	errs chan error,
) (<-chan datastore.RevisionChanges, <-chan error) {
	if options.EmissionStrategy == datastore.EmitImmediatelyStrategy &&
		(options.Content&datastore.WatchCheckpoints != datastore.WatchCheckpoints) {
		close(updates)
		errs <- errors.New("EmitImmediatelyStrategy requires WatchCheckpoints to be set")
		return updates, errs
	}

	afterRevision, ok := afterRevisionRaw.(postgresRevision)
	if !ok {
		close(updates)
		errs <- datastore.NewInvalidRevisionErr(afterRevisionRaw, datastore.CouldNotDetermineRevision)
		return updates, errs
	}

	watchBufferWriteTimeout := options.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = pgd.watchBufferWriteTimeout
	}

	sendChange := func(change datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true

		default:
			// If we cannot immediately write, set up the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return true

		case <-timer.C:
			errs <- datastore.NewWatchDisconnectedErr()
			return false
		}
	}

	go func() {
		defer close(updates)
		defer close(errs)

		err := pgd.runCursorWatch(ctx, afterRevision, options, sendChange)
		if err == nil {
			return
		}

		if errors.Is(err, errCursorWatchDisconnected) {
			// The disconnection error was already delivered by sendChange.
			return
		}

		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			errs <- datastore.NewWatchCanceledErr()
		case common.IsCancellationError(err):
			errs <- datastore.NewWatchCanceledErr()
		case common.IsResettableError(err):
			errs <- datastore.NewWatchTemporaryErr(err)
		default:
			errs <- err
		}
	}()

	return updates, errs
}

// runCursorWatch resolves the caller's starting position, then polls the
// frontier-bounded discovery query, emitting each batch and its checkpoint,
// until the context ends or a guard fails.
func (pgd *pgDatastore) runCursorWatch(
	ctx context.Context,
	afterRevision postgresRevision,
	options datastore.WatchOptions,
	sendChange func(datastore.RevisionChanges) bool,
) error {
	batchSize := max(pgd.watchBatchSize, 1)
	pollInterval := max(pgd.watchPollInterval, minimumCursorWatchPollInterval)
	checkpointInterval := max(options.CheckpointInterval, minimumWatchSleep)
	requestedCheckpoints := options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints
	livenessTimeout := max(pgd.logicalWatchLedgerWaitTimeout, ledgerFrontierProbeInterval)

	// Before anything is delivered: a revision outside the garbage collection
	// window names transactions that may already be gone.
	if err := pgd.checkWatchRevisionRetained(ctx, afterRevision); err != nil {
		return err
	}

	start, err := pgd.beginCursorWatch(ctx, afterRevision, options, sendChange)
	if err != nil {
		return err
	}

	cursor := start.cursor
	checkpointSnapshot := start.snapshot
	checkpointNanos := afterRevision.optionalInexactNanosTimestamp
	lastCheckpointLSN := cursor
	lastCheckpointAt := time.Now()

	// inactiveSince tracks how long the ledger slot has been without a writer.
	// Brief detachment is routine (the writer died and another instance is
	// taking over); only a sustained one fails the watch.
	var inactiveSince time.Time

	for {
		if ctx.Err() != nil {
			return datastore.NewWatchCanceledErr()
		}

		state, err := pgd.readLedgerSlotState(ctx)
		if err != nil {
			return err
		}

		// Liveness guard: with no frontier there is no basis for delivery, and
		// with no writer the frontier will not move. Failing is deliberate; the
		// alternative is a silent stall indistinguishable from "no writes".
		if !state.exists {
			return datastore.NewWatchTemporaryErr(fmt.Errorf(
				"the commit LSN ledger's replication slot %q does not exist, so the watch has no delivery frontier",
				pgd.ledgerSlotName))
		}
		if state.active {
			inactiveSince = time.Time{}
		} else {
			if inactiveSince.IsZero() {
				inactiveSince = time.Now()
			}
			if time.Since(inactiveSince) > livenessTimeout {
				return datastore.NewWatchTemporaryErr(fmt.Errorf(
					"the commit LSN ledger has had no writer for %s: slot %q is confirmed through %s, attached=%t, wal_status=%q",
					livenessTimeout, pgd.ledgerSlotName, state.confirmed, state.active, state.walStatus))
			}
		}

		frontier := state.confirmed
		if frontier > cursor {
			watchFrontierLagGauge.Set(float64(frontier - cursor))
		} else {
			watchFrontierLagGauge.Set(0)
		}

		if frontier > cursor {
			// Gap guard: a recorded gap above the cursor means transactions this
			// watch is responsible for were never recorded, so what the frontier
			// covers is not what was delivered. It is checked here, before every
			// delivery and before every idle checkpoint, because those are the
			// only two ways the omission could be passed off as completeness: a
			// slot recreation moves the frontier past the gap in one step, and an
			// unguarded poll would step over it in silence.
			if err := pgd.checkLedgerGapAbove(ctx, cursor); err != nil {
				return err
			}

			startedAt := time.Now()
			revisions, err := pgd.getRecordedRevisions(ctx, cursor, frontier, batchSize)
			if err != nil {
				return err
			}
			watchPollDurationHistogram.Observe(time.Since(startedAt).Seconds())
			watchBatchTransactionsHistogram.Observe(float64(len(revisions)))

			if len(revisions) > 0 {
				checkpoint, err := pgd.emitRevisionBatch(ctx, revisions, options, sendChange)
				if err != nil {
					return err
				}

				cursor = pglogrepl.LSN(checkpoint.optionalCommitLSN)
				checkpointSnapshot = checkpoint.snapshot
				checkpointNanos = checkpoint.optionalInexactNanosTimestamp
				lastCheckpointLSN = cursor
				lastCheckpointAt = time.Now()

				if len(revisions) == batchSize {
					// A full batch means a backlog: drain it at query speed
					// rather than one batch per poll interval.
					continue
				}
			} else if requestedCheckpoints && frontier > lastCheckpointLSN && time.Since(lastCheckpointAt) >= checkpointInterval {
				// Nothing new committed, but delivery is provably complete
				// through the frontier, so consumers watching a subset of
				// content still observe progress. The checkpoint carries the
				// last delivered transaction's snapshot: pg_current_snapshot()
				// would cover transactions in (frontier, now] that have NOT
				// been delivered, and feeding such a token to a
				// snapshot-filtered path (e.g. the polling watch) would lose
				// them silently.
				if !sendChange(datastore.RevisionChanges{
					Revision: postgresRevision{
						snapshot:                      checkpointSnapshot,
						optionalInexactNanosTimestamp: checkpointNanos,
						optionalCommitLSN:             uint64(frontier),
					},
					IsCheckpoint: true,
				}) {
					return errCursorWatchDisconnected
				}
				lastCheckpointLSN = frontier
				lastCheckpointAt = time.Now()
			}
		}

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return datastore.NewWatchCanceledErr()
		}
	}
}

// cursorWatchStart is the resolved starting state of a cursor watch.
type cursorWatchStart struct {
	// cursor is the position delivery resumes strictly above.
	cursor pglogrepl.LSN

	// snapshot seeds the idle checkpoints' snapshot component until the first
	// delivery replaces it.
	snapshot pgSnapshot
}

// beginCursorWatch resolves the caller's afterRevision into a cursor.
//
// A position-carrying revision is the normal case: the cursor is its commit
// LSN, complete on its own, because every stream delivers in commit order and
// the consumer received everything at or below it before the token was minted.
// The snapshot the token carries is not consulted for delivery at all.
//
// A snapshot-only revision (a polling-watch token, or a HeadRevision taken
// before the feature was enabled) requires a one-time backfill phase:
//
//  1. Take a snapshot S. Write a marker transaction and wait until the ledger
//     records its commit position. The marker commits after S, so the probe
//     proves every transaction visible in S has its position recorded — except
//     those predating the ledger, which the genesis snapshot classifies.
//  2. Deliver the transactions visible in S but not in the caller's snapshot,
//     ordered by recorded position with the pre-ledger prefix first, in
//     batches, each with its checkpoint.
//  3. Hand off to the cursor loop at the highest recorded position visible in
//     S. Visibility in S partitions transactions by commit time and commit
//     time order is commit LSN order, so everything not visible in S — the
//     loop's responsibility — lies strictly above the handoff cursor: no gap
//     and no duplicate at the boundary.
//
// The probe in step 1 is what closes the accounting. Without it, a transaction
// visible in S but not yet recorded at query time would be indistinguishable
// from a slot-recreation gap (a spurious hard error), and, worse, the handoff
// cursor could be read below such a transaction's eventual position, letting
// the loop deliver it again after the backfill phase's snapshot filter had
// already skipped it as seen.
//
// The handoff position in step 3 is the highest recorded position *visible in
// S*, and deliberately not the ledger's confirmed frontier. The frontier can
// have advanced past S while the backfill ran, and a transaction that committed
// in between is neither visible in S (so the backfill does not deliver it) nor
// above the frontier (so a loop starting there would skip it): handing off at
// the frontier loses exactly those transactions.
func (pgd *pgDatastore) beginCursorWatch(
	ctx context.Context,
	afterRevision postgresRevision,
	options datastore.WatchOptions,
	sendChange func(datastore.RevisionChanges) bool,
) (cursorWatchStart, error) {
	if afterRevision.ByteSortable() {
		return cursorWatchStart{
			cursor:   pglogrepl.LSN(afterRevision.optionalCommitLSN),
			snapshot: afterRevision.snapshot,
		}, nil
	}

	var backfillSnapshot pgSnapshot
	if err := pgd.readPool.QueryRow(ctx, "SELECT pg_current_snapshot();").Scan(&backfillSnapshot); err != nil {
		return cursorWatchStart{}, fmt.Errorf("unable to load the backfill snapshot: %w", err)
	}

	markerXid, err := pgd.writeWatchMarker(ctx)
	if err != nil {
		return cursorWatchStart{}, datastore.NewWatchTemporaryErr(err)
	}

	if err := pgd.awaitLedgerFrontier(ctx, markerXid); err != nil {
		return cursorWatchStart{}, err
	}

	// The ledger recorded the marker, so the slot existed a moment ago; confirm
	// it still does before delivering anything, so that a watch whose frontier
	// has just disappeared fails here rather than mid-backfill.
	state, err := pgd.readLedgerSlotState(ctx)
	if err != nil {
		return cursorWatchStart{}, err
	}
	if !state.exists {
		return cursorWatchStart{}, datastore.NewWatchTemporaryErr(fmt.Errorf(
			"the commit LSN ledger's replication slot %q does not exist, so the watch has no delivery frontier",
			pgd.ledgerSlotName))
	}

	revisions, err := pgd.getLegacyCatchupRevisions(ctx, afterRevision, backfillSnapshot)
	if err != nil {
		return cursorWatchStart{}, err
	}

	start := cursorWatchStart{snapshot: afterRevision.snapshot}

	batchSize := max(pgd.watchBatchSize, 1)
	for offset := 0; offset < len(revisions); offset += batchSize {
		chunk := revisions[offset:min(offset+batchSize, len(revisions))]
		checkpoint, err := pgd.emitRevisionBatch(ctx, chunk, options, sendChange)
		if err != nil {
			return cursorWatchStart{}, err
		}
		start.snapshot = checkpoint.snapshot
	}

	var handoffText *string
	if err := pgd.readPool.QueryRow(ctx, maxVisibleRecordedCommitLSNQuery, backfillSnapshot).Scan(&handoffText); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return cursorWatchStart{}, fmt.Errorf("unable to determine the backfill handoff position: %w", err)
		}
	}
	if handoffText != nil {
		handoff, err := pglogrepl.ParseLSN(*handoffText)
		if err != nil {
			return cursorWatchStart{}, fmt.Errorf("unable to parse the backfill handoff position: %w", err)
		}
		start.cursor = handoff
	}

	return start, nil
}

// writeWatchMarker commits the marker transaction and returns its transaction ID.
func (pgd *pgDatastore) writeWatchMarker(ctx context.Context) (xid8, error) {
	var markerXid xid8
	if err := pgd.writePool.QueryRow(ctx, insertWatchMarkerQuery).Scan(&markerXid); err != nil {
		return markerXid, fmt.Errorf("unable to write the watch marker transaction: %w", err)
	}
	return markerXid, nil
}

// awaitLedgerFrontier waits until the commit LSN ledger has recorded the
// marker's commit position. The ledger records positions in commit order, so
// once the marker has one, so does every transaction that committed before it.
//
// This is the only point at which a starting Watch call blocks on the ledger.
// Failing here is deliberate: without it, the backfill could neither classify
// unrecorded transactions nor read a handoff position it can trust.
func (pgd *pgDatastore) awaitLedgerFrontier(ctx context.Context, markerXid xid8) error {
	waitTimeout := max(pgd.logicalWatchLedgerWaitTimeout, ledgerFrontierProbeInterval)
	startedAt := time.Now()

	deadlineCtx, cancelWait := context.WithTimeout(ctx, waitTimeout)
	defer cancelWait()

	for {
		// The marker having no row yet is the ordinary case being waited on; any
		// other failure is real.
		var commitLSNText string
		err := pgd.readPool.QueryRow(deadlineCtx, commitLSNForXidQuery, markerXid).Scan(&commitLSNText)
		switch {
		case err == nil:
			watchLedgerWaitHistogram.Observe(time.Since(startedAt).Seconds())
			return nil

		case errors.Is(err, pgx.ErrNoRows):
			// Not recorded yet; fall through to wait.

		default:
			if ctx.Err() == nil && deadlineCtx.Err() != nil {
				return pgd.ledgerFrontierTimeoutError(ctx, markerXid, waitTimeout)
			}
			return fmt.Errorf("unable to read the watch marker's recorded commit position: %w", err)
		}

		select {
		case <-deadlineCtx.Done():
			if ctx.Err() != nil {
				return datastore.NewWatchCanceledErr()
			}
			return pgd.ledgerFrontierTimeoutError(ctx, markerXid, waitTimeout)
		case <-time.After(ledgerFrontierProbeInterval):
		}
	}
}

// ledgerFrontierTimeoutError reports a stalled ledger with the state an operator
// needs to act on: which slot is behind, how far it has confirmed, and whether
// any instance is attached to it at all.
func (pgd *pgDatastore) ledgerFrontierTimeoutError(ctx context.Context, markerXid xid8, waited time.Duration) error {
	state, err := pgd.readLedgerSlotState(ctx)
	if err != nil {
		return fmt.Errorf(
			"the commit LSN ledger did not record the watch marker transaction %d within %s, and its slot %q could not be inspected: %w",
			markerXid.Uint64, waited, pgd.ledgerSlotName, err)
	}

	if !state.exists {
		return fmt.Errorf(
			"the commit LSN ledger did not record the watch marker transaction %d within %s: its replication slot %q does not exist",
			markerXid.Uint64, waited, pgd.ledgerSlotName)
	}

	return fmt.Errorf(
		"the commit LSN ledger did not record the watch marker transaction %d within %s: slot %q is confirmed through %s, attached=%t, wal_status=%q",
		markerXid.Uint64, waited, pgd.ledgerSlotName, state.confirmed, state.active, state.walStatus)
}

// checkLedgerGapAbove fails the watch when a recorded ledger gap lies above the
// given position: the gap's transactions were never recorded, so delivery from
// this position cannot be complete. The failure is permanent for this cursor —
// the consumer must restart from a current revision — so it is not wrapped as
// temporary.
func (pgd *pgDatastore) checkLedgerGapAbove(ctx context.Context, position pglogrepl.LSN) error {
	gap, found, err := pgd.firstLedgerGapAbove(ctx, position)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	watchGapRejectionsCounter.Inc()
	return fmt.Errorf(
		"the commit LSN ledger's replication slot was recreated at %s and transactions that committed in (%s, %s] have no recorded commit position; a watch positioned at %s cannot be resumed across the gap and must restart from a current revision",
		gap.detectedAt, gap.from, gap.to, position)
}

// checkWatchRevisionRetained fails a starting watch whose revision names a
// transaction older than the garbage collection window. Everything at that
// position is collectable, so the transactions the consumer has not seen may
// already be gone, and no stream can deliver them. Reporting that is an
// improvement on both existing watches, which serve such a consumer a silently
// truncated stream.
//
// The test is on the revision's own recorded timestamp against the database's
// clock, which is the same basis garbage collection deletes on. Two other
// formulations were tried and are unsound:
//
//   - CheckRevision's snapshot comparison against the oldest in-window
//     transaction calls a revision stale whenever the oldest surviving
//     transaction is newer than it, which on any database without history older
//     than the window (a fresh one, for instance) is every revision.
//   - "the cursor is below the oldest recorded commit position" cannot tell
//     transactions collected from above the cursor apart from there having been
//     none, so it fails a consumer that is perfectly caught up when collection
//     prunes below it.
//
// A revision with no timestamp (a HeadRevision, or an idle checkpoint minted
// before any delivery) is exempt: there is nothing to compare, and no evidence
// of loss.
func (pgd *pgDatastore) checkWatchRevisionRetained(ctx context.Context, afterRevision postgresRevision) error {
	revisionNanos, ok := afterRevision.OptionalNanosTimestamp()
	if !ok {
		return nil
	}

	var now time.Time
	if err := pgd.readPool.QueryRow(ctx, "SELECT NOW() AT TIME ZONE 'utc';").Scan(&now); err != nil {
		return fmt.Errorf("unable to determine the database time: %w", err)
	}

	nanos, err := safecast.Convert[uint64](now.Add(-pgd.gcWindow).UnixNano())
	if err != nil {
		return fmt.Errorf("unable to determine the retained transaction window: %w", err)
	}

	if revisionNanos < nanos {
		watchStaleRevisionCounter.Inc()
		return datastore.NewInvalidRevisionErr(afterRevision, datastore.RevisionStale)
	}

	return nil
}

// getRecordedRevisions returns the transactions whose recorded commit positions
// lie in (after, upTo], in commit order, at most batchSize of them.
func (pgd *pgDatastore) getRecordedRevisions(ctx context.Context, after, upTo pglogrepl.LSN, batchSize int) ([]postgresRevision, error) {
	rows, err := pgd.readPool.Query(ctx, cursorWatchRevisionsQuery, after.String(), upTo.String(), batchSize)
	if err != nil {
		return nil, fmt.Errorf("unable to load recorded revisions: %w", err)
	}
	defer rows.Close()

	var revisions []postgresRevision
	for rows.Next() {
		revision, commitLSNText, err := scanWatchRevisionRow(rows)
		if err != nil {
			return nil, err
		}
		if commitLSNText == nil {
			return nil, spiceerrors.MustBugf("the cursor discovery query returned a transaction with no recorded position")
		}

		commitLSN, err := pglogrepl.ParseLSN(*commitLSNText)
		if err != nil {
			return nil, fmt.Errorf("unable to decode the recorded commit position of transaction %d: %w", revision.optionalTxID.Uint64, err)
		}
		revision.optionalCommitLSN = uint64(commitLSN)

		revisions = append(revisions, revision)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("unable to load recorded revisions: %w", rows.Err())
	}

	return revisions, nil
}

// getLegacyCatchupRevisions returns the transactions visible in `upTo` but not
// in afterRevision, in commit order, each carrying the commit position the
// ledger recorded for it, with the pre-ledger prefix unpositioned.
func (pgd *pgDatastore) getLegacyCatchupRevisions(ctx context.Context, afterRevision postgresRevision, upTo pgSnapshot) ([]postgresRevision, error) {
	var revisions []postgresRevision
	unrecorded := false
	if err := pgx.BeginTxFunc(ctx, pgd.readPool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, catchupRevisionsQuery, afterRevision.snapshot, upTo)
		if err != nil {
			return fmt.Errorf("unable to load catch-up revisions: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			revision, commitLSNText, err := scanWatchRevisionRow(rows)
			if err != nil {
				return err
			}

			if commitLSNText == nil {
				unrecorded = true
			} else {
				commitLSN, err := pglogrepl.ParseLSN(*commitLSNText)
				if err != nil {
					return fmt.Errorf("unable to decode the recorded commit position of transaction %d: %w", revision.optionalTxID.Uint64, err)
				}
				revision.optionalCommitLSN = uint64(commitLSN)
			}

			revisions = append(revisions, revision)
		}
		if rows.Err() != nil {
			return fmt.Errorf("unable to load catch-up revisions: %w", rows.Err())
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	if unrecorded {
		if err := pgd.checkUnrecordedCatchupRevisions(ctx, revisions, afterRevision); err != nil {
			return nil, err
		}
	}

	return revisions, nil
}

// scanWatchRevisionRow decodes one transaction-table row of the shape shared by
// the discovery and backfill queries into a revision, returning the recorded
// commit position's text form (or nil) for the caller to interpret.
func scanWatchRevisionRow(rows pgx.Rows) (postgresRevision, *string, error) {
	var nextXID xid8
	var nextSnapshot pgSnapshot
	var metadata map[string]any
	var timestamp time.Time
	var commitLSNText *string
	if err := rows.Scan(&nextXID, &nextSnapshot, &metadata, &timestamp, &commitLSNText); err != nil {
		return postgresRevision{}, nil, fmt.Errorf("unable to decode watch revision: %w", err)
	}

	nanosTimestamp, err := safecast.Convert[uint64](timestamp.UnixNano())
	if err != nil {
		return postgresRevision{}, nil, fmt.Errorf("could not cast timestamp to uint64: %w", err)
	}

	return postgresRevision{
		snapshot:                      nextSnapshot.markComplete(nextXID.Uint64),
		optionalTxID:                  nextXID,
		optionalInexactNanosTimestamp: nanosTimestamp,
		optionalMetadata:              metadata,
	}, commitLSNText, nil
}

// checkUnrecordedCatchupRevisions decides what to do about replayable
// transactions that have no recorded commit position. There are exactly two
// reasons for that, and the ledger's genesis snapshot tells them apart:
//
//   - The transaction committed before the ledger existed, so it is visible in
//     the genesis snapshot. It really did commit before everything the ledger
//     recorded, and the query sorts it accordingly, so it is emitted as an
//     unpositioned revision: the same token the polling watch would emit.
//
//   - The transaction committed while the ledger's slot was invalid, so it is not
//     visible in the genesis snapshot. Its commit position is unrecoverable, so
//     the watch refuses rather than guessing one.
//
// A caller resuming from a position-carrying revision cannot be served an
// unpositioned one either way, because it would not be comparable with the token
// the caller already holds.
func (pgd *pgDatastore) checkUnrecordedCatchupRevisions(ctx context.Context, revisions []postgresRevision, afterRevision postgresRevision) error {
	genesis, err := pgd.ledgerGenesisSnapshot(ctx)
	if err != nil {
		return err
	}

	for _, revision := range revisions {
		if revision.ByteSortable() {
			continue
		}

		xid := revision.optionalTxID.Uint64
		if !genesis.txVisible(xid) {
			return fmt.Errorf(
				"transaction %d has no recorded commit LSN and postdates the commit LSN ledger's genesis, which means the ledger's replication slot %q was recreated and this transaction's commit position is unrecoverable; watches cannot be ordered across the gap and must restart from a current revision",
				xid, pgd.ledgerSlotName)
		}

		if afterRevision.ByteSortable() {
			return fmt.Errorf(
				"transaction %d predates the commit LSN ledger and so has no position to emit, but this watch resumed from a position-carrying revision; restart the watch from a current revision",
				xid)
		}
	}

	return nil
}

// emitRevisionBatch loads and sends the changes for one batch of transactions,
// already in commit order, followed by the batch's checkpoint. The returned
// checkpoint revision sits at the last transaction, with every transaction of
// the batch marked complete in its snapshot, mirroring the polling watch's
// per-batch checkpoints.
func (pgd *pgDatastore) emitRevisionBatch(
	ctx context.Context,
	revisions []postgresRevision,
	options datastore.WatchOptions,
	sendChange func(datastore.RevisionChanges) bool,
) (postgresRevision, error) {
	changesToWrite, err := pgd.loadChanges(ctx, revisions, options)
	if err != nil {
		return postgresRevision{}, err
	}

	for change, err := range changesToWrite {
		if err != nil {
			return postgresRevision{}, err
		}

		if options.EmissionStrategy == datastore.EmitImmediatelyStrategy {
			for _, atom := range decomposeRevisionChanges(change) {
				if !sendChange(atom) {
					return postgresRevision{}, errCursorWatchDisconnected
				}
			}
			continue
		}

		if !sendChange(change) {
			return postgresRevision{}, errCursorWatchDisconnected
		}
	}

	lastRevision := revisions[len(revisions)-1]
	checkpoint := postgresRevision{
		snapshot:                      lastRevision.snapshot,
		optionalTxID:                  lastRevision.optionalTxID,
		optionalInexactNanosTimestamp: lastRevision.optionalInexactNanosTimestamp,
		optionalCommitLSN:             lastRevision.optionalCommitLSN,
	}
	for _, revision := range revisions {
		checkpoint.snapshot = checkpoint.snapshot.markComplete(revision.optionalTxID.Uint64)
	}

	if options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
		if !sendChange(datastore.RevisionChanges{
			Revision:     checkpoint,
			IsCheckpoint: true,
		}) {
			return postgresRevision{}, errCursorWatchDisconnected
		}
	}

	return checkpoint, nil
}

// decomposeRevisionChanges splits an assembled RevisionChanges into independent
// single-item events for the EmitImmediatelyStrategy.
func decomposeRevisionChanges(change datastore.RevisionChanges) []datastore.RevisionChanges {
	atoms := make([]datastore.RevisionChanges, 0,
		len(change.RelationshipChanges)+len(change.ChangedDefinitions)+len(change.DeletedNamespaces)+len(change.DeletedCaveats)+len(change.Metadatas))

	for _, relChange := range change.RelationshipChanges {
		atoms = append(atoms, datastore.RevisionChanges{
			Revision:            change.Revision,
			RelationshipChanges: []tuple.RelationshipUpdate{relChange},
		})
	}
	for _, def := range change.ChangedDefinitions {
		atoms = append(atoms, datastore.RevisionChanges{
			Revision:           change.Revision,
			ChangedDefinitions: []datastore.SchemaDefinition{def},
		})
	}
	for _, namespaceName := range change.DeletedNamespaces {
		atoms = append(atoms, datastore.RevisionChanges{
			Revision:          change.Revision,
			DeletedNamespaces: []string{namespaceName},
		})
	}
	for _, caveatName := range change.DeletedCaveats {
		atoms = append(atoms, datastore.RevisionChanges{
			Revision:       change.Revision,
			DeletedCaveats: []string{caveatName},
		})
	}
	for _, metadata := range change.Metadatas {
		atoms = append(atoms, datastore.RevisionChanges{
			Revision:  change.Revision,
			Metadatas: []*structpb.Struct{metadata},
		})
	}

	return atoms
}
