package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	watchSleep = 100 * time.Millisecond
)

type revisionWithXid struct {
	postgresRevision
	tx xid8
}

var (
	// This query must cast an xid8 to xid, which is a safe operation as long as the
	// xid8 is one of the last ~2 billion transaction IDs generated. We should be garbage
	// collecting these transactions long before we get to that point.
	newRevisionsQuery = fmt.Sprintf(`
	SELECT %[1]s, %[2]s FROM %[3]s
	WHERE %[1]s >= pg_snapshot_xmax($1) OR (
		%[1]s >= pg_snapshot_xmin($1) AND NOT pg_visible_in_snapshot(%[1]s, $1)
	) ORDER BY pg_xact_commit_timestamp(%[1]s::xid), %[1]s;`, colXID, colSnapshot, tableTransaction)

	queryChanged = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
		colCreatedXid,
		colDeletedXid,
	).From(tableTuple)
)

func (pgd *pgDatastore) Watch(
	ctx context.Context,
	afterRevisionRaw datastore.Revision,
) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, pgd.watchBufferLength)
	errs := make(chan error, 1)

	if !pgd.watchEnabled {
		errs <- datastore.NewWatchDisabledErr("postgres must be run with track_commit_timestamp=on for watch to be enabled. See https://spicedb.dev/d/enable-watch-api-postgres")
		return updates, errs
	}

	afterRevision := afterRevisionRaw.(postgresRevision)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := afterRevision

		for {
			newTxns, err := pgd.getNewRevisions(ctx, currentTxn)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}

			if len(newTxns) > 0 {
				changesToWrite, err := pgd.loadChanges(ctx, newTxns)
				if err != nil {
					if errors.Is(ctx.Err(), context.Canceled) {
						errs <- datastore.NewWatchCanceledErr()
					} else {
						errs <- err
					}
					return
				}

				for _, changeToWrite := range changesToWrite {
					changeToWrite := changeToWrite

					select {
					case updates <- &changeToWrite:
						// Nothing to do here, we've already written to the channel.
					default:
						errs <- datastore.NewWatchDisconnectedErr()
						return
					}

					currentTxn = changeToWrite.Revision.(revisionWithXid).postgresRevision
				}
			} else {
				sleep := time.NewTimer(watchSleep)

				select {
				case <-sleep.C:
					break
				case <-ctx.Done():
					errs <- datastore.NewWatchCanceledErr()
					return
				}
			}
		}
	}()

	return updates, errs
}

func (pgd *pgDatastore) getNewRevisions(
	ctx context.Context,
	afterTX postgresRevision,
) ([]revisionWithXid, error) {
	var ids []revisionWithXid
	if err := pgd.dbpool.BeginTxFunc(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, newRevisionsQuery, afterTX.snapshot)
		if err != nil {
			return fmt.Errorf("unable to load new revisions: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var nextXID xid8
			var nextSnapshot pgSnapshot
			if err := rows.Scan(&nextXID, &nextSnapshot); err != nil {
				return fmt.Errorf("unable to decode new revision: %w", err)
			}

			ids = append(ids, revisionWithXid{
				postgresRevision{nextSnapshot.markComplete(nextXID.Uint)},
				nextXID,
			})
		}
		if rows.Err() != nil {
			return fmt.Errorf("unable to load new revisions: %w", err)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	return ids, nil
}

func (pgd *pgDatastore) loadChanges(ctx context.Context, revisions []revisionWithXid) ([]datastore.RevisionChanges, error) {
	min := revisions[0].tx.Uint
	max := revisions[0].tx.Uint
	filter := make(map[uint64]int, len(revisions))
	txidToRevision := make(map[uint64]revisionWithXid, len(revisions))

	for i, rev := range revisions {
		if rev.tx.Uint < min {
			min = rev.tx.Uint
		}
		if rev.tx.Uint > max {
			max = rev.tx.Uint
		}
		filter[rev.tx.Uint] = i
		txidToRevision[rev.tx.Uint] = rev
	}

	sql, args, err := queryChanged.Where(sq.Or{
		sq.And{
			sq.LtOrEq{colCreatedXid: max},
			sq.GtOrEq{colCreatedXid: min},
		},
		sq.And{
			sq.LtOrEq{colDeletedXid: max},
			sq.GtOrEq{colDeletedXid: min},
		},
	}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("unable to prepare changes SQL: %w", err)
	}

	changes, err := pgd.dbpool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to load changes for XID: %w", err)
	}

	tracked := common.NewChanges(revisionKeyFunc)
	for changes.Next() {
		nextTuple := &core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}

		var createdXID, deletedXID xid8
		var caveatName string
		var caveatContext map[string]any
		if err := changes.Scan(
			&nextTuple.ResourceAndRelation.Namespace,
			&nextTuple.ResourceAndRelation.ObjectId,
			&nextTuple.ResourceAndRelation.Relation,
			&nextTuple.Subject.Namespace,
			&nextTuple.Subject.ObjectId,
			&nextTuple.Subject.Relation,
			&caveatName,
			&caveatContext,
			&createdXID,
			&deletedXID,
		); err != nil {
			return nil, fmt.Errorf("unable to parse changed tuple: %w", err)
		}

		if caveatName != "" {
			contextStruct, err := structpb.NewStruct(caveatContext)
			if err != nil {
				return nil, fmt.Errorf("failed to read caveat context from update: %w", err)
			}
			nextTuple.Caveat = &core.ContextualizedCaveat{
				CaveatName: caveatName,
				Context:    contextStruct,
			}
		}

		if _, found := filter[createdXID.Uint]; found {
			tracked.AddChange(ctx, txidToRevision[createdXID.Uint], nextTuple, core.RelationTupleUpdate_TOUCH)
		}
		if _, found := filter[deletedXID.Uint]; found {
			tracked.AddChange(ctx, txidToRevision[deletedXID.Uint], nextTuple, core.RelationTupleUpdate_DELETE)
		}
	}
	if changes.Err() != nil {
		return nil, fmt.Errorf("unable to load changes for XID: %w", err)
	}

	reconciledChanges := tracked.AsRevisionChanges(func(lhs, rhs uint64) bool {
		return filter[lhs] < filter[rhs]
	})
	return reconciledChanges, nil
}
