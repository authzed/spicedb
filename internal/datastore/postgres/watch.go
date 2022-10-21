package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	watchSleep = 100 * time.Millisecond
)

var (
	// This query must cast an xid8 to xid, which is a safe operation as long as the
	// xid8 is one of the last ~2 billion transaction IDs generated. We should be garbage
	// collecting these transactions long before we get to that point.
	newRevisionsQuery = fmt.Sprintf(`
	SELECT %[1]s from %[2]s
	WHERE pg_xact_commit_timestamp(%[1]s::xid) > (
		SELECT pg_xact_commit_timestamp(%[1]s::xid) FROM relation_tuple_transaction where %[1]s = $1
	) AND %[1]s < pg_snapshot_xmin(pg_current_snapshot())
	ORDER BY pg_xact_commit_timestamp(%[1]s::xid);
`, colXID, tableTransaction)

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
	afterRevision datastore.Revision,
) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, pgd.watchBufferLength)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := transactionFromRevision(afterRevision)

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

			for _, revision := range newTxns {
				changeToWrite, err := pgd.loadChanges(ctx, revision)
				if err != nil {
					if errors.Is(ctx.Err(), context.Canceled) {
						errs <- datastore.NewWatchCanceledErr()
					} else {
						errs <- err
					}
					return
				}

				select {
				case updates <- changeToWrite:
				default:
					errs <- datastore.NewWatchDisconnectedErr()
					return
				}

				currentTxn = revision
			}

			if len(newTxns) == 0 {
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
	afterTX xid8,
) ([]xid8, error) {
	rows, err := pgd.dbpool.Query(context.Background(), newRevisionsQuery, afterTX)
	if err != nil {
		return nil, fmt.Errorf("unable to load new revisions: %w", err)
	}
	defer rows.Close()

	var ids []xid8
	for rows.Next() {
		var nextXID xid8
		if err := rows.Scan(&nextXID); err != nil {
			return nil, fmt.Errorf("unable to decode new revision: %w", err)
		}

		ids = append(ids, nextXID)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("unable to load new revisions: %w", err)
	}

	return ids, nil
}

func (pgd *pgDatastore) loadChanges(ctx context.Context, revision xid8) (*datastore.RevisionChanges, error) {
	sql, args, err := queryChanged.Where(sq.Or{
		sq.Eq{colCreatedXid: revision},
		sq.Eq{colDeletedXid: revision},
	}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("unable to prepare changes SQL: %w", err)
	}

	changes, err := pgd.dbpool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to load changes for XID: %w", err)
	}

	tracked := common.NewChanges()
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

		if createdXID.Uint == revision.Uint {
			tracked.AddChange(ctx, revisionFromTransaction(revision, revision), nextTuple, core.RelationTupleUpdate_TOUCH)
		} else if deletedXID.Uint == revision.Uint {
			tracked.AddChange(ctx, revisionFromTransaction(revision, revision), nextTuple, core.RelationTupleUpdate_DELETE)
		}
	}
	if changes.Err() != nil {
		return nil, fmt.Errorf("unable to load changes for XID: %w", err)
	}

	reconciledChanges := tracked.AsRevisionChanges()
	if len(reconciledChanges) == 0 {
		return &datastore.RevisionChanges{
			Revision: revisionFromTransaction(revision, revision),
		}, nil
	}
	return reconciledChanges[0], nil
}
