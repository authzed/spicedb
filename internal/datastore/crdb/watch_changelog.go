package crdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// changelogSelectColumns is the ordered column list read from the changelog on
// each poll. It must match the scan order in accumulateChangelogRows.
const changelogSelectColumns = schema.ColChangeTS + ", " + schema.ColChangeKind + ", " +
	schema.ColNamespace + ", " + schema.ColObjectID + ", " + schema.ColRelation + ", " +
	schema.ColUsersetNamespace + ", " + schema.ColUsersetObjectID + ", " + schema.ColUsersetRelation + ", " +
	schema.ColCaveatContextName + ", " + schema.ColCaveatContext + ", " +
	schema.ColChangeRelExpiration + ", " + schema.ColChangeOperation + ", " +
	schema.ColChangeSchemaKind + ", " + schema.ColChangeDefinitionName + ", " + schema.ColChangeSerializedDefinition + ", " +
	schema.ColChangeMetadata

// changelogImmediateSelectColumns is changelogSelectColumns prefixed with the
// row ordinal, used only by the immediate-emission path so it can dedup on the
// (change_ts, ordinal) primary key. It must match the scan order in
// accumulateChangelogRowsImmediate.
const changelogImmediateSelectColumns = schema.ColChangeOrdinal + ", " + changelogSelectColumns

// changelogRowKey identifies a single changelog row by its primary key
// (change_ts, ordinal). It is used as the dedup-set key for the provisional
// window in immediate-emission mode.
type changelogRowKey struct {
	changeTS string
	ordinal  int64
}

// watchViaChangelog serves Watch by repeatedly polling the append-only
// changelog table at present time, instead of consuming a CRDB changefeed.
//
// Each poll runs a read-only transaction and reads cluster_logical_timestamp()
// (clusterNow) once. Because the changelog is append-only, a present read
// already observes every row committed with change_ts <= clusterNow regardless
// of changefeed health -- no AS OF SYSTEM TIME is needed -- which is what lets
// this path survive bulk loads.
//
// Correctness (no lost updates) lives in the COMPLETENESS CURSOR, not in the
// individual change rows. A checkpoint at revision C promises "you have seen
// every change <= C", and that promise is only safe once no straggler can
// still commit at change_ts <= C. Under the cluster's --max-offset clock skew
// bound that safe point is safeTS = clusterNow - maxOffset. The cursor (the
// lower bound carried to the next poll, and the value returned as the new
// cursor) therefore advances ONLY to safeTS, never to clusterNow: a
// transaction not yet visible cluster-wide commits above safeTS and is caught
// by a later poll rather than skipped.
//
// Emission strategy is honored:
//   - datastore.EmitImmediatelyStrategy: each row in (cursor, clusterNow] is
//     emitted the instant it is read (change latency ≈ the poll/nudge
//     interval), deduped within the provisional window (safeTS, clusterNow] via
//     the (change_ts, ordinal) primary key. Duplicates/reordering across polls
//     are permitted by the Watch contract and minimized by that dedup set.
//   - datastore.EmitWhenCheckpointedStrategy (default): only (cursor, safeTS]
//     is read, buffered/deduped via common.NewChanges and released
//     grouped-by-revision with the checkpoint -- never early, never reordered.
//
// Either way a checkpoint is emitted at safeTS and the cursor advances to
// safeTS. Checkpoints advance on the ticker even when idle, since clusterNow
// (and thus safeTS) is re-read every poll.
func (cds *crdbDatastore) watchViaChangelog(
	ctx context.Context,
	afterRevision datastore.Revision,
	opts datastore.WatchOptions,
	updates chan datastore.RevisionChanges,
	errs chan error,
) {
	defer close(updates)
	defer close(errs)

	watchConnectTimeout := opts.WatchConnectTimeout
	if watchConnectTimeout <= 0 {
		watchConnectTimeout = cds.watchConnectTimeout
	}

	// Use a dedicated, non-pooled connection for watch, mirroring the changefeed
	// path.
	conn, err := pgxcommon.ConnectWithInstrumentationAndTimeout(ctx, cds.dburl, watchConnectTimeout)
	if err != nil {
		errs <- err
		return
	}
	defer func() { go func() { _ = conn.Close(ctx) }() }()

	interval := 1 * time.Second
	if opts.CheckpointInterval > 0 {
		interval = opts.CheckpointInterval
	}

	watchBufferWriteTimeout := opts.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = cds.watchBufferWriteTimeout
	}

	sendChange := func(change datastore.RevisionChanges) error {
		select {
		case updates <- change:
			return nil
		default:
			// If we cannot immediately write, set up the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return nil
		case <-timer.C:
			return datastore.NewWatchDisconnectedErr()
		}
	}

	sendError := func(err error) {
		if errors.Is(ctx.Err(), context.Canceled) {
			errs <- datastore.NewWatchCanceledErr()
			return
		}

		if strings.Contains(err.Error(), "must be after replica GC threshold") {
			errs <- datastore.NewInvalidRevisionErr(afterRevision, datastore.RevisionStale)
			return
		}

		if pool.IsResettableError(ctx, err) || pool.IsRetryableError(ctx, err) {
			errs <- datastore.NewWatchTemporaryErr(err)
			return
		}

		errs <- err
	}

	// The cursor is the last revision we have fully emitted; the next poll reads
	// strictly after it.
	hlcAfter, ok := afterRevision.(revisions.HLCRevision)
	if !ok {
		sendError(spiceerrors.MustBugf("expected HLCRevision for changelog watch, got %T", afterRevision))
		return
	}
	cursor, err := hlcAfter.AsDecimal()
	if err != nil {
		sendError(fmt.Errorf("invalid afterRevision for changelog watch: %w", err))
		return
	}

	// Guard against a cursor older than the changelog's GC/TTL window: rows
	// covering that span may already have been reaped, so polling forward
	// from it could silently skip history rather than replay it. Fail fast
	// with the same stale-revision error the changefeed watch path surfaces
	// (via the "must be after replica GC threshold" mapping in sendError
	// above) instead of relying solely on the present read to notice.
	cursorTime := time.Unix(0, hlcAfter.TimestampNanoSec())
	if time.Since(cursorTime) > cds.gcWindow {
		sendError(datastore.NewInvalidRevisionErr(afterRevision, datastore.RevisionStale))
		return
	}

	watchBufferSize := opts.MaximumBufferedChangesByteSize
	if watchBufferSize == 0 {
		watchBufferSize = cds.watchChangeBufferMaximumSize
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// The nudge is a purely optional low-latency wake for the poll loop: a
	// lightweight changefeed on the changelog table whose payload we never
	// inspect. Any row event is a signal to poll sooner than the next tick.
	// The ticker above remains the correctness backstop -- if the nudge
	// changefeed stalls or fails to start (e.g. during the exact bulk-load
	// scenario this feature routes around), polling still advances on the
	// timer, so nothing here may affect correctness, only latency.
	nudge := make(chan struct{}, 1)
	go cds.runChangelogNudge(ctx, nudge)

	immediate := opts.EmissionStrategy == datastore.EmitImmediatelyStrategy

	// emittedWindow is the provisional-window dedup set for immediate mode:
	// (change_ts, ordinal) keys already emitted in a poll whose clusterNow ran
	// ahead of the cursor. Entries are pruned once the cursor advances past
	// their change_ts (see pollChangelogImmediate). It is unused in buffered
	// mode.
	emittedWindow := make(map[changelogRowKey]struct{})

	maxOffsetNanos := cds.changelogWatchMaxOffset.Nanoseconds()

	for {
		newCursor, err := cds.pollChangelogOnce(ctx, conn, opts, cursor, maxOffsetNanos, watchBufferSize, immediate, emittedWindow, sendChange)
		if err != nil {
			sendError(err)
			return
		}
		cursor = newCursor

		select {
		case <-ctx.Done():
			sendError(ctx.Err())
			return
		case <-ticker.C:
		case <-nudge:
		}
	}
}

// runChangelogNudge opens a best-effort CRDB changefeed on the changelog
// table purely to wake watchViaChangelog's poll loop early on new activity.
// The changefeed payload is never parsed -- any row event is a signal to
// poll sooner. This is strictly a latency optimization: the poll loop's
// ticker is the correctness backstop, so any failure here is logged and the
// goroutine simply exits without affecting the watch.
func (cds *crdbDatastore) runChangelogNudge(ctx context.Context, nudge chan<- struct{}) {
	conn, err := pgxcommon.ConnectWithInstrumentationAndTimeout(ctx, cds.dburl, cds.watchConnectTimeout)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("changelog nudge changefeed unavailable; falling back to interval polling")
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	head, err := cds.HeadRevision(ctx)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("changelog nudge changefeed could not determine head revision; falling back to interval polling")
		return
	}

	query := fmt.Sprintf(cds.beginChangefeedQuery, schema.TableRelationshipChangelog, head.Revision, "1s")
	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("changelog nudge changefeed failed to start; falling back to interval polling")
		return
	}
	defer rows.Close()

	for rows.Next() {
		// Non-blocking: the poll loop only needs to know that *something*
		// happened, not how many times. If a nudge is already pending, drop
		// this one rather than blocking the changefeed read loop.
		select {
		case nudge <- struct{}{}:
		default:
		}
	}
	if err := rows.Err(); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
		log.Ctx(ctx).Warn().Err(err).Msg("changelog nudge changefeed ended with an error; falling back to interval polling")
	}
}

// safeTSFromClusterNow returns clusterNow - maxOffsetNanos. clusterNow is an
// HLC decimal whose integer part is nanoseconds since the Unix epoch (see
// HLCRevision.TimestampNanoSec and the HLCRevision decimal layout), so
// subtracting maxOffsetNanos (also in nanoseconds) yields the completeness
// floor safeTS at the same logical position. Subtracting from the decimal
// directly preserves the fractional logical-clock component of clusterNow.
func safeTSFromClusterNow(clusterNow decimal.Decimal, maxOffsetNanos int64) decimal.Decimal {
	return clusterNow.Sub(decimal.NewFromInt(maxOffsetNanos))
}

// pollChangelogOnce runs one poll against the append-only changelog. It reads
// cluster_logical_timestamp() (clusterNow) in a single read-only transaction,
// derives safeTS = clusterNow - maxOffset, emits changes and (when requested) a
// checkpoint at safeTS, and returns the new cursor.
//
// INV1 (no lost updates): the returned cursor advances only to safeTS, never to
// clusterNow. Immediate mode reads up to clusterNow for low change latency, but
// anything in (safeTS, clusterNow] stays > cursor and is re-read next poll (so
// stragglers committing late with a low change_ts are never skipped); the
// provisional-window dedup set keeps that re-read from re-delivering a row the
// client already saw.
func (cds *crdbDatastore) pollChangelogOnce(
	ctx context.Context,
	conn *pgx.Conn,
	opts datastore.WatchOptions,
	cursor decimal.Decimal,
	maxOffsetNanos int64,
	watchBufferSize uint64,
	immediate bool,
	emittedWindow map[changelogRowKey]struct{},
	sendChange sendChangeFunc,
) (decimal.Decimal, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return cursor, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Read the cluster clock and the changelog rows from the same read-only
	// transaction so clusterNow and the rows come from one consistent point.
	// No AS OF SYSTEM TIME: the changelog is append-only, so a present read
	// already sees every row with change_ts <= clusterNow regardless of
	// changefeed health.
	var clusterNow decimal.Decimal
	if err := tx.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&clusterNow); err != nil {
		return cursor, err
	}

	safeTS := safeTSFromClusterNow(clusterNow, maxOffsetNanos)

	var newCursor decimal.Decimal
	if immediate {
		newCursor, err = cds.pollChangelogImmediate(ctx, tx, opts, cursor, clusterNow, safeTS, emittedWindow, sendChange)
	} else {
		newCursor, err = cds.pollChangelogBuffered(ctx, tx, opts, cursor, safeTS, watchBufferSize, sendChange)
	}
	if err != nil {
		return cursor, err
	}

	if err := tx.Commit(ctx); err != nil {
		return cursor, err
	}

	return newCursor, nil
}

// pollChangelogImmediate implements EmitImmediatelyStrategy: every changelog
// row in (cursor, clusterNow] is emitted the instant it is read, so change
// latency is ≈ the poll/nudge interval rather than the checkpoint interval.
//
// Because clusterNow > safeTS, this poll may read rows in the provisional
// window (safeTS, clusterNow] that a later poll will re-read (the cursor only
// advances to safeTS). emittedWindow tracks the (change_ts, ordinal) keys
// already delivered so those re-reads do not re-emit them; it is pruned of
// keys <= the new cursor once the cursor advances. Duplicates/reordering are
// permitted by the Watch contract for immediate mode, but this keeps them
// minimal.
//
// The cursor advances only to max(cursor, safeTS) (INV1). A checkpoint is
// emitted at safeTS once safeTS > cursor.
func (cds *crdbDatastore) pollChangelogImmediate(
	ctx context.Context,
	tx pgx.Tx,
	opts datastore.WatchOptions,
	cursor decimal.Decimal,
	clusterNow decimal.Decimal,
	safeTS decimal.Decimal,
	emittedWindow map[changelogRowKey]struct{},
	sendChange sendChangeFunc,
) (decimal.Decimal, error) {
	// Emit changes as they are read, preserving Create (no touch normalization)
	// and per-row ordering. streamingChangeProvider emits on each Add* call.
	streamer := &streamingChangeProvider{
		sendChange: sendChange,
		content:    opts.Content,
	}

	if clusterNow.GreaterThan(cursor) {
		query := fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s > $1 AND %s <= $2 ORDER BY %s, %s",
			changelogImmediateSelectColumns, schema.TableRelationshipChangelog,
			schema.ColChangeTS, schema.ColChangeTS, schema.ColChangeTS, schema.ColChangeOrdinal,
		)
		rows, err := tx.Query(ctx, query, cursor, clusterNow)
		if err != nil {
			return cursor, err
		}
		if err := cds.accumulateChangelogRowsImmediate(ctx, rows, streamer, emittedWindow); err != nil {
			return cursor, err
		}
	}

	// Advance the completeness cursor only to safeTS, then checkpoint there.
	newCursor := cursor
	if safeTS.GreaterThan(cursor) {
		newCursor = safeTS
		if err := cds.emitChangelogCheckpoint(opts, safeTS, sendChange); err != nil {
			return cursor, err
		}
	}

	// Prune the dedup set of any key whose change_ts is now <= the cursor: those
	// rows will never be re-read (the next poll's lower bound is > newCursor), so
	// they can no longer be duplicated.
	for key := range emittedWindow {
		keyTS, err := decimal.NewFromString(key.changeTS)
		if err != nil {
			return cursor, err
		}
		if keyTS.LessThanOrEqual(newCursor) {
			delete(emittedWindow, key)
		}
	}

	return newCursor, nil
}

// pollChangelogBuffered implements the default EmitWhenCheckpointedStrategy: it
// reads only the settled window (cursor, safeTS], buffers/dedups it via
// common.NewChanges, and releases it grouped-by-revision, in revision order,
// with the checkpoint at safeTS. Nothing is emitted early or out of order
// (INV4).
func (cds *crdbDatastore) pollChangelogBuffered(
	ctx context.Context,
	tx pgx.Tx,
	opts datastore.WatchOptions,
	cursor decimal.Decimal,
	safeTS decimal.Decimal,
	watchBufferSize uint64,
	sendChange sendChangeFunc,
) (decimal.Decimal, error) {
	// safeTS has not yet advanced past the cursor (e.g. just started at HEAD, or
	// idle within one maxOffset window). Emit nothing but keep the idle-checkpoint
	// behavior: a checkpoint at the (unadvanced) cursor lets consumers know the
	// stream is live without regressing completeness.
	if safeTS.LessThanOrEqual(cursor) {
		return cursor, cds.emitChangelogCheckpoint(opts, cursor, sendChange)
	}

	if err := cds.pollChangelogRange(ctx, tx, opts, cursor, safeTS, watchBufferSize, sendChange); err != nil {
		return cursor, err
	}
	return safeTS, nil
}

// pollChangelogRange reads all changelog rows in (cursor, target] using the
// provided transaction, emits them grouped by revision, and emits a checkpoint
// at target when checkpoints are requested. target is passed in explicitly so
// tests can pin it to a known value; production passes safeTS.
//
// Emission is INCLUSIVE of target: the SQL bound is change_ts <= target and the
// tracker is created fresh for this range, so every accumulated row is
// guaranteed to satisfy cursor < change_ts <= target. We therefore emit
// everything accumulated (AsRevisionChanges) rather than the strict "< target"
// filter, which would drop any row whose change_ts exactly equals target.
//
// Callers must ensure target > cursor; the idle/just-started case is handled by
// pollChangelogBuffered.
func (cds *crdbDatastore) pollChangelogRange(
	ctx context.Context,
	tx pgx.Tx,
	opts datastore.WatchOptions,
	cursor decimal.Decimal,
	target decimal.Decimal,
	watchBufferSize uint64,
	sendChange sendChangeFunc,
) error {
	tracked := common.NewChanges(revisions.HLCKeyFunc, opts.Content, watchBufferSize)

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s > $1 AND %s <= $2 ORDER BY %s, %s",
		changelogSelectColumns, schema.TableRelationshipChangelog,
		schema.ColChangeTS, schema.ColChangeTS, schema.ColChangeTS, schema.ColChangeOrdinal,
	)
	rows, err := tx.Query(ctx, query, cursor, target)
	if err != nil {
		return err
	}
	if err := cds.accumulateChangelogRows(ctx, rows, tracked); err != nil {
		return err
	}

	// Emit every accumulated change in ascending revision order. Every row read
	// is by construction in (cursor, target], so emitting all of them is exactly
	// the (cursor, target] window, inclusive of target.
	for revChange, err := range tracked.AsRevisionChanges(revisions.HLCKeyLessThanFunc) {
		if err != nil {
			return err
		}
		if err := sendChange(revChange); err != nil {
			return err
		}
	}

	return cds.emitChangelogCheckpoint(opts, target, sendChange)
}

// emitChangelogCheckpoint emits a checkpoint at target if WatchCheckpoints is set.
func (cds *crdbDatastore) emitChangelogCheckpoint(opts datastore.WatchOptions, target decimal.Decimal, sendChange sendChangeFunc) error {
	if opts.Content&datastore.WatchCheckpoints != datastore.WatchCheckpoints {
		return nil
	}
	rev, err := revisions.NewForHLC(target)
	if err != nil {
		return err
	}
	return sendChange(datastore.RevisionChanges{Revision: rev, IsCheckpoint: true})
}

// changelogRowFields holds the reconstructed non-key columns of a changelog
// row, shared by the buffered and immediate scan paths.
type changelogRowFields struct {
	kind                         string
	nsName, objectID, relation   *string
	usNs, usObjectID, usRelation *string
	caveatName                   *string
	caveatContext                map[string]any
	relExpiration                *time.Time
	operation                    *string
	schemaKind, definitionName   *string
	serializedDefinition         []byte
	metadata                     map[string]any
}

// feedChangelogRow reconstructs one changelog row and feeds it to the tracker.
//
// When normalizeCreate is true, a "create" operation is reported as Touch. The
// shared change tracker (internal/datastore/common.Changes, used by the
// buffered path and by the Postgres/MySQL watch implementations) only
// distinguishes Touch vs Delete — at the row level a physical insert is
// indistinguishable from a touch of a previously nonexistent row — so those
// backends report inserts as Touch. The immediate path emits through
// streamingChangeProvider, which does represent Create distinctly, so it passes
// normalizeCreate=false to preserve it.
func feedChangelogRow(ctx context.Context, tracked changeTracker[revisions.HLCRevision, revisions.HLCRevision], rev revisions.HLCRevision, f changelogRowFields, normalizeCreate bool) error {
	switch f.kind {
	case "rel":
		ctxCaveat, err := common.ContextualizedCaveatFrom(deref(f.caveatName), f.caveatContext)
		if err != nil {
			return err
		}
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{ObjectType: deref(f.nsName), ObjectID: deref(f.objectID), Relation: deref(f.relation)},
				Subject:  tuple.ObjectAndRelation{ObjectType: deref(f.usNs), ObjectID: deref(f.usObjectID), Relation: deref(f.usRelation)},
			},
			OptionalCaveat:     ctxCaveat,
			OptionalExpiration: f.relExpiration,
		}
		op, err := changelogOperation(deref(f.operation))
		if err != nil {
			return err
		}
		if normalizeCreate && op == tuple.UpdateOperationCreate {
			op = tuple.UpdateOperationTouch
		}
		return tracked.AddRelationshipChange(ctx, rev, rel, op)
	case "schema":
		if f.serializedDefinition != nil {
			switch deref(f.schemaKind) {
			case "namespace":
				def := &core.NamespaceDefinition{}
				if err := def.UnmarshalVT(f.serializedDefinition); err != nil {
					return err
				}
				return tracked.AddChangedDefinition(ctx, rev, def)
			case "caveat":
				def := &core.CaveatDefinition{}
				if err := def.UnmarshalVT(f.serializedDefinition); err != nil {
					return err
				}
				return tracked.AddChangedDefinition(ctx, rev, def)
			default:
				return spiceerrors.MustBugf("unknown schema_kind in changelog: %s", deref(f.schemaKind))
			}
		}
		switch deref(f.schemaKind) {
		case "namespace":
			return tracked.AddDeletedNamespace(ctx, rev, deref(f.definitionName))
		case "caveat":
			return tracked.AddDeletedCaveat(ctx, rev, deref(f.definitionName))
		default:
			return spiceerrors.MustBugf("unknown schema_kind in changelog: %s", deref(f.schemaKind))
		}
	case "metadata":
		// Metadata emission is independent of opts.Content, matching the
		// changefeed path (AddRevisionMetadata is not content-gated). We never
		// wrote $spicedbTransactionKey in changelog mode, so unlike the
		// changefeed path there is no key to strip here.
		if len(f.metadata) > 0 {
			return tracked.AddRevisionMetadata(ctx, rev, f.metadata)
		}
		return nil
	default:
		return spiceerrors.MustBugf("unknown changelog kind: %s", f.kind)
	}
}

// accumulateChangelogRows scans changelog rows into the change tracker. The
// tracker itself applies the WatchContent filtering, so both relationship and
// schema rows are always fed in. Used by the buffered path, so create is
// normalized to touch.
func (cds *crdbDatastore) accumulateChangelogRows(ctx context.Context, rows pgx.Rows, tracked changeTracker[revisions.HLCRevision, revisions.HLCRevision]) error {
	defer rows.Close()
	for rows.Next() {
		var changeTS decimal.Decimal
		var f changelogRowFields
		if err := rows.Scan(&changeTS, &f.kind, &f.nsName, &f.objectID, &f.relation,
			&f.usNs, &f.usObjectID, &f.usRelation, &f.caveatName, &f.caveatContext,
			&f.relExpiration, &f.operation, &f.schemaKind, &f.definitionName, &f.serializedDefinition,
			&f.metadata); err != nil {
			return err
		}

		rev, err := revisions.NewForHLC(changeTS)
		if err != nil {
			return err
		}
		if err := feedChangelogRow(ctx, tracked, rev, f, true); err != nil {
			return err
		}
	}
	return rows.Err()
}

// accumulateChangelogRowsImmediate scans changelog rows (prefixed with the
// ordinal, per changelogImmediateSelectColumns) and emits each through the
// streaming provider the instant it is read, skipping any (change_ts, ordinal)
// already delivered in the provisional window. Newly-emitted keys are recorded
// in emittedWindow; the caller prunes it as the cursor advances. Create is
// preserved (normalizeCreate=false).
func (cds *crdbDatastore) accumulateChangelogRowsImmediate(ctx context.Context, rows pgx.Rows, streamer changeTracker[revisions.HLCRevision, revisions.HLCRevision], emittedWindow map[changelogRowKey]struct{}) error {
	defer rows.Close()
	for rows.Next() {
		var ordinal int64
		var changeTS decimal.Decimal
		var f changelogRowFields
		if err := rows.Scan(&ordinal, &changeTS, &f.kind, &f.nsName, &f.objectID, &f.relation,
			&f.usNs, &f.usObjectID, &f.usRelation, &f.caveatName, &f.caveatContext,
			&f.relExpiration, &f.operation, &f.schemaKind, &f.definitionName, &f.serializedDefinition,
			&f.metadata); err != nil {
			return err
		}

		key := changelogRowKey{changeTS: changeTS.String(), ordinal: ordinal}
		if _, ok := emittedWindow[key]; ok {
			// Already delivered in a previous poll's provisional window.
			continue
		}

		rev, err := revisions.NewForHLC(changeTS)
		if err != nil {
			return err
		}
		if err := feedChangelogRow(ctx, streamer, rev, f, false); err != nil {
			return err
		}
		emittedWindow[key] = struct{}{}
	}
	return rows.Err()
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// changelogOperation maps the stored operation string back to a
// tuple.UpdateOperation. Any unrecognized value is fatal; there is no valid
// zero/unknown operation, so callers must treat a non-nil error as fatal.
func changelogOperation(s string) (tuple.UpdateOperation, error) {
	switch s {
	case "create":
		return tuple.UpdateOperationCreate, nil
	case "touch":
		return tuple.UpdateOperationTouch, nil
	case "delete":
		return tuple.UpdateOperationDelete, nil
	default:
		return tuple.UpdateOperationTouch, fmt.Errorf("unknown changelog operation: %s", s)
	}
}
