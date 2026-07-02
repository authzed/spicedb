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
	schema.ColChangeSchemaKind + ", " + schema.ColChangeDefinitionName + ", " + schema.ColChangeSerializedDefinition

// watchViaChangelog serves Watch by repeatedly polling the changelog table at a
// guaranteed-closed past timestamp, instead of consuming a CRDB changefeed.
//
// Each poll runs a read-only transaction pinned to follower_read_timestamp(),
// reads cluster_logical_timestamp() as the closed target, and selects every
// changelog row in (cursor, target]. Because the read is at a closed timestamp,
// it observes every committed write with commit ts <= target regardless of
// changefeed health, which is what lets this path survive bulk loads.
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

	watchBufferSize := opts.MaximumBufferedChangesByteSize
	if watchBufferSize == 0 {
		watchBufferSize = cds.watchChangeBufferMaximumSize
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		newCursor, err := cds.pollChangelogOnce(ctx, conn, opts, cursor, watchBufferSize, sendChange)
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
		}
	}
}

// pollChangelogOnce reads all changelog rows in (cursor, target] at a closed
// timestamp, emits them grouped by revision, emits a checkpoint at target when
// checkpoints are requested, and returns target as the new cursor.
func (cds *crdbDatastore) pollChangelogOnce(
	ctx context.Context,
	conn *pgx.Conn,
	opts datastore.WatchOptions,
	cursor decimal.Decimal,
	watchBufferSize uint64,
	sendChange sendChangeFunc,
) (decimal.Decimal, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return cursor, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Pin the transaction to a guaranteed-closed past timestamp. All reads in
	// this tx see every committed write with commit ts <= target, regardless of
	// changefeed health. This is the mechanism that survives bulk loads.
	if _, err := tx.Exec(ctx, "SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()"); err != nil {
		return cursor, err
	}

	var target decimal.Decimal
	if err := tx.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&target); err != nil {
		return cursor, err
	}

	// Nothing new since last poll: still emit a checkpoint so consumers advance.
	if target.LessThanOrEqual(cursor) {
		if err := cds.emitChangelogCheckpoint(opts, target, sendChange); err != nil {
			return cursor, err
		}
		return cursor, tx.Commit(ctx)
	}

	tracked := common.NewChanges(revisions.HLCKeyFunc, opts.Content, watchBufferSize)

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s > $1 AND %s <= $2 ORDER BY %s, %s",
		changelogSelectColumns, schema.TableRelationshipChangelog,
		schema.ColChangeTS, schema.ColChangeTS, schema.ColChangeTS, schema.ColChangeOrdinal,
	)
	rows, err := tx.Query(ctx, query, cursor, target)
	if err != nil {
		return cursor, err
	}
	if err := cds.accumulateChangelogRows(ctx, rows, tracked); err != nil {
		return cursor, err
	}

	targetRev, err := revisions.NewForHLC(target)
	if err != nil {
		return cursor, err
	}
	filtered := tracked.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, targetRev)
	for revChange, err := range filtered {
		if err != nil {
			return cursor, err
		}
		if err := sendChange(revChange); err != nil {
			return cursor, err
		}
	}

	if err := cds.emitChangelogCheckpoint(opts, target, sendChange); err != nil {
		return cursor, err
	}

	return target, tx.Commit(ctx)
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

// accumulateChangelogRows scans changelog rows into the change tracker. The
// tracker itself applies the WatchContent filtering, so both relationship and
// schema rows are always fed in.
func (cds *crdbDatastore) accumulateChangelogRows(ctx context.Context, rows pgx.Rows, tracked changeTracker[revisions.HLCRevision, revisions.HLCRevision]) error {
	defer rows.Close()
	for rows.Next() {
		var changeTS decimal.Decimal
		var kind string
		var nsName, objectID, relation, usNs, usObjectID, usRelation *string
		var caveatName *string
		var caveatContext map[string]any
		var relExpiration *time.Time
		var operation *string
		var schemaKind, definitionName *string
		var serializedDefinition []byte

		if err := rows.Scan(&changeTS, &kind, &nsName, &objectID, &relation,
			&usNs, &usObjectID, &usRelation, &caveatName, &caveatContext,
			&relExpiration, &operation, &schemaKind, &definitionName, &serializedDefinition); err != nil {
			return err
		}

		rev, err := revisions.NewForHLC(changeTS)
		if err != nil {
			return err
		}

		switch kind {
		case "rel":
			ctxCaveat, err := common.ContextualizedCaveatFrom(deref(caveatName), caveatContext)
			if err != nil {
				return err
			}
			rel := tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{ObjectType: deref(nsName), ObjectID: deref(objectID), Relation: deref(relation)},
					Subject:  tuple.ObjectAndRelation{ObjectType: deref(usNs), ObjectID: deref(usObjectID), Relation: deref(usRelation)},
				},
				OptionalCaveat:     ctxCaveat,
				OptionalExpiration: relExpiration,
			}
			op, err := changelogOperation(deref(operation))
			if err != nil {
				return err
			}
			if err := tracked.AddRelationshipChange(ctx, rev, rel, op); err != nil {
				return err
			}
		case "schema":
			if serializedDefinition != nil {
				switch deref(schemaKind) {
				case "namespace":
					def := &core.NamespaceDefinition{}
					if err := def.UnmarshalVT(serializedDefinition); err != nil {
						return err
					}
					if err := tracked.AddChangedDefinition(ctx, rev, def); err != nil {
						return err
					}
				case "caveat":
					def := &core.CaveatDefinition{}
					if err := def.UnmarshalVT(serializedDefinition); err != nil {
						return err
					}
					if err := tracked.AddChangedDefinition(ctx, rev, def); err != nil {
						return err
					}
				default:
					return spiceerrors.MustBugf("unknown schema_kind in changelog: %s", deref(schemaKind))
				}
			} else {
				switch deref(schemaKind) {
				case "namespace":
					if err := tracked.AddDeletedNamespace(ctx, rev, deref(definitionName)); err != nil {
						return err
					}
				case "caveat":
					if err := tracked.AddDeletedCaveat(ctx, rev, deref(definitionName)); err != nil {
						return err
					}
				default:
					return spiceerrors.MustBugf("unknown schema_kind in changelog: %s", deref(schemaKind))
				}
			}
		default:
			return spiceerrors.MustBugf("unknown changelog kind: %s", kind)
		}
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
