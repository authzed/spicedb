package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	minimumWatchSleep = 100 * time.Millisecond
)

var (
	// This query must cast an xid8 to xid, which is a safe operation as long as the
	// xid8 is one of the last ~2 billion transaction IDs generated. We should be garbage
	// collecting these transactions long before we get to that point.
	newRevisionsQuery = fmt.Sprintf(`
	SELECT %[1]s, %[2]s, %[3]s FROM %[4]s
	WHERE %[1]s >= pg_snapshot_xmax($1) OR (
		%[1]s >= pg_snapshot_xmin($1) AND NOT pg_visible_in_snapshot(%[1]s, $1)
	) ORDER BY pg_xact_commit_timestamp(%[1]s::xid), %[1]s;`, colXID, colSnapshot, colTimestamp, tableTransaction)

	queryChangedTuples = psql.Select(
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

	queryChangedNamespaces = psql.Select(
		colConfig,
		colCreatedXid,
		colDeletedXid,
	).From(tableNamespace)

	queryChangedCaveats = psql.Select(
		colCaveatName,
		colCaveatDefinition,
		colCreatedXid,
		colDeletedXid,
	).From(tableCaveat)
)

func (pgd *pgDatastore) Watch(
	ctx context.Context,
	afterRevisionRaw datastore.Revision,
	options datastore.WatchOptions,
) (<-chan *datastore.RevisionChanges, <-chan error) {
	watchBufferLength := options.WatchBufferLength
	if watchBufferLength <= 0 {
		watchBufferLength = pgd.watchBufferLength
	}

	updates := make(chan *datastore.RevisionChanges, watchBufferLength)
	errs := make(chan error, 1)

	if !pgd.watchEnabled {
		errs <- datastore.NewWatchDisabledErr("postgres must be run with track_commit_timestamp=on for watch to be enabled. See https://spicedb.dev/d/enable-watch-api-postgres")
		return updates, errs
	}

	afterRevision := afterRevisionRaw.(postgresRevision)
	watchSleep := options.CheckpointInterval
	if watchSleep < minimumWatchSleep {
		watchSleep = minimumWatchSleep
	}

	watchBufferWriteTimeout := options.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = pgd.watchBufferWriteTimeout
	}

	sendChange := func(change *datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true

		default:
			// If we cannot immediately write, setup the timer and try again.
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

		currentTxn := afterRevision

		for {
			newTxns, err := pgd.getNewRevisions(ctx, currentTxn)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else if pgxcommon.IsCancellationError(err) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}

			if len(newTxns) > 0 {
				changesToWrite, err := pgd.loadChanges(ctx, newTxns, options)
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
					if !sendChange(&changeToWrite) {
						return
					}
				}

				// In order to make progress, we need to ensure that any seen transactions here are
				// marked as done in the revision given back to Postgres on the next iteration. We pick
				// the *last* transaction to start, as it should encompass all completed transactions
				// except those running concurrently, which is handled by calling markComplete on the other
				// transactions.
				currentTxn = newTxns[len(newTxns)-1]
				for _, newTx := range newTxns {
					currentTxn = postgresRevision{
						snapshot:               currentTxn.snapshot.markComplete(newTx.optionalTxID.Uint64),
						optionalTxID:           currentTxn.optionalTxID,
						optionalNanosTimestamp: currentTxn.optionalNanosTimestamp,
					}
				}

				// If checkpoints were requested, output a checkpoint. While the Postgres datastore does not
				// move revisions forward outside of changes, these could be necessary if the caller is
				// watching only a *subset* of changes.
				if options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
					if !sendChange(&datastore.RevisionChanges{
						Revision:     currentTxn,
						IsCheckpoint: true,
					}) {
						return
					}
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

func (pgd *pgDatastore) getNewRevisions(ctx context.Context, afterTX postgresRevision) ([]postgresRevision, error) {
	var ids []postgresRevision
	if err := pgx.BeginTxFunc(ctx, pgd.readPool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, newRevisionsQuery, afterTX.snapshot)
		if err != nil {
			return fmt.Errorf("unable to load new revisions: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var nextXID xid8
			var nextSnapshot pgSnapshot
			var timestamp time.Time
			if err := rows.Scan(&nextXID, &nextSnapshot, &timestamp); err != nil {
				return fmt.Errorf("unable to decode new revision: %w", err)
			}

			nanosTimestamp, err := safecast.ToUint64(timestamp.UnixNano())
			if err != nil {
				return spiceerrors.MustBugf("could not cast timestamp to uint64")
			}

			ids = append(ids, postgresRevision{
				snapshot:               nextSnapshot.markComplete(nextXID.Uint64),
				optionalTxID:           nextXID,
				optionalNanosTimestamp: nanosTimestamp,
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

func (pgd *pgDatastore) loadChanges(ctx context.Context, revisions []postgresRevision, options datastore.WatchOptions) ([]datastore.RevisionChanges, error) {
	xmin := revisions[0].optionalTxID.Uint64
	xmax := revisions[0].optionalTxID.Uint64
	filter := make(map[uint64]int, len(revisions))
	txidToRevision := make(map[uint64]postgresRevision, len(revisions))

	for i, rev := range revisions {
		if rev.optionalTxID.Uint64 < xmin {
			xmin = rev.optionalTxID.Uint64
		}
		if rev.optionalTxID.Uint64 > xmax {
			xmax = rev.optionalTxID.Uint64
		}
		filter[rev.optionalTxID.Uint64] = i
		txidToRevision[rev.optionalTxID.Uint64] = rev
	}

	tracked := common.NewChanges(revisionKeyFunc, options.Content, options.MaximumBufferedChangesByteSize)

	// Load relationship changes.
	if options.Content&datastore.WatchRelationships == datastore.WatchRelationships {
		err := pgd.loadRelationshipChanges(ctx, xmin, xmax, txidToRevision, filter, tracked)
		if err != nil {
			return nil, err
		}
	}

	// Load namespace changes.
	if options.Content&datastore.WatchSchema == datastore.WatchSchema {
		err := pgd.loadNamespaceChanges(ctx, xmin, xmax, txidToRevision, filter, tracked)
		if err != nil {
			return nil, err
		}
	}

	// Load caveat changes.
	if options.Content&datastore.WatchSchema == datastore.WatchSchema {
		err := pgd.loadCaveatChanges(ctx, xmin, xmax, txidToRevision, filter, tracked)
		if err != nil {
			return nil, err
		}
	}

	// Reconcile the changes.
	reconciledChanges := tracked.AsRevisionChanges(func(lhs, rhs uint64) bool {
		return filter[lhs] < filter[rhs]
	})
	return reconciledChanges, nil
}

func (pgd *pgDatastore) loadRelationshipChanges(ctx context.Context, xmin uint64, xmax uint64, txidToRevision map[uint64]postgresRevision, filter map[uint64]int, tracked *common.Changes[postgresRevision, uint64]) error {
	sql, args, err := queryChangedTuples.Where(sq.Or{
		sq.And{
			sq.LtOrEq{colCreatedXid: xmax},
			sq.GtOrEq{colCreatedXid: xmin},
		},
		sq.And{
			sq.LtOrEq{colDeletedXid: xmax},
			sq.GtOrEq{colDeletedXid: xmin},
		},
	}).ToSql()
	if err != nil {
		return fmt.Errorf("unable to prepare changes SQL: %w", err)
	}

	changes, err := pgd.readPool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("unable to load changes for XID: %w", err)
	}

	defer changes.Close()

	for changes.Next() {
		nextTuple := &core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}

		var createdXID, deletedXID xid8
		var caveatName *string
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
			return fmt.Errorf("unable to parse changed tuple: %w", err)
		}

		if caveatName != nil && *caveatName != "" {
			contextStruct, err := structpb.NewStruct(caveatContext)
			if err != nil {
				return fmt.Errorf("failed to read caveat context from update: %w", err)
			}
			nextTuple.Caveat = &core.ContextualizedCaveat{
				CaveatName: *caveatName,
				Context:    contextStruct,
			}
		}

		if _, found := filter[createdXID.Uint64]; found {
			if err := tracked.AddRelationshipChange(ctx, txidToRevision[createdXID.Uint64], nextTuple, core.RelationTupleUpdate_TOUCH); err != nil {
				return err
			}
		}
		if _, found := filter[deletedXID.Uint64]; found {
			if err := tracked.AddRelationshipChange(ctx, txidToRevision[deletedXID.Uint64], nextTuple, core.RelationTupleUpdate_DELETE); err != nil {
				return err
			}
		}
	}
	if changes.Err() != nil {
		return fmt.Errorf("unable to load changes for XID: %w", err)
	}
	return nil
}

func (pgd *pgDatastore) loadNamespaceChanges(ctx context.Context, xmin uint64, xmax uint64, txidToRevision map[uint64]postgresRevision, filter map[uint64]int, tracked *common.Changes[postgresRevision, uint64]) error {
	sql, args, err := queryChangedNamespaces.Where(sq.Or{
		sq.And{
			sq.LtOrEq{colCreatedXid: xmax},
			sq.GtOrEq{colCreatedXid: xmin},
		},
		sq.And{
			sq.LtOrEq{colDeletedXid: xmax},
			sq.GtOrEq{colDeletedXid: xmin},
		},
	}).ToSql()
	if err != nil {
		return fmt.Errorf("unable to prepare changes SQL: %w", err)
	}

	changes, err := pgd.readPool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("unable to load changes for XID: %w", err)
	}

	defer changes.Close()

	for changes.Next() {
		var createdXID, deletedXID xid8
		var config []byte
		if err := changes.Scan(
			&config,
			&createdXID,
			&deletedXID,
		); err != nil {
			return fmt.Errorf("unable to parse changed namespace: %w", err)
		}

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(config); err != nil {
			return fmt.Errorf(errUnableToReadConfig, err)
		}

		if _, found := filter[createdXID.Uint64]; found {
			err := tracked.AddChangedDefinition(ctx, txidToRevision[deletedXID.Uint64], loaded)
			if err != nil {
				return err
			}
		}
		if _, found := filter[deletedXID.Uint64]; found {
			err := tracked.AddDeletedNamespace(ctx, txidToRevision[deletedXID.Uint64], loaded.Name)
			if err != nil {
				return err
			}
		}
	}
	if changes.Err() != nil {
		return fmt.Errorf("unable to load changes for XID: %w", err)
	}
	return nil
}

func (pgd *pgDatastore) loadCaveatChanges(ctx context.Context, minimum uint64, maximum uint64, txidToRevision map[uint64]postgresRevision, filter map[uint64]int, tracked *common.Changes[postgresRevision, uint64]) error {
	sql, args, err := queryChangedCaveats.Where(sq.Or{
		sq.And{
			sq.LtOrEq{colCreatedXid: maximum},
			sq.GtOrEq{colCreatedXid: minimum},
		},
		sq.And{
			sq.LtOrEq{colDeletedXid: maximum},
			sq.GtOrEq{colDeletedXid: minimum},
		},
	}).ToSql()
	if err != nil {
		return fmt.Errorf("unable to prepare changes SQL: %w", err)
	}

	changes, err := pgd.readPool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("unable to load changes for XID: %w", err)
	}

	defer changes.Close()

	for changes.Next() {
		var createdXID, deletedXID xid8
		var config []byte
		var name string
		if err := changes.Scan(
			&name,
			&config,
			&createdXID,
			&deletedXID,
		); err != nil {
			return fmt.Errorf("unable to parse changed caveat: %w", err)
		}

		loaded := &core.CaveatDefinition{}
		if err := loaded.UnmarshalVT(config); err != nil {
			return fmt.Errorf(errUnableToReadConfig, err)
		}

		if _, found := filter[createdXID.Uint64]; found {
			err := tracked.AddChangedDefinition(ctx, txidToRevision[deletedXID.Uint64], loaded)
			if err != nil {
				return err
			}
		}
		if _, found := filter[deletedXID.Uint64]; found {
			err := tracked.AddDeletedCaveat(ctx, txidToRevision[deletedXID.Uint64], loaded.Name)
			if err != nil {
				return err
			}
		}
	}
	if changes.Err() != nil {
		return fmt.Errorf("unable to load changes for XID: %w", err)
	}
	return nil
}
