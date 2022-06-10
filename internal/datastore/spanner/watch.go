package spanner

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	watchSleep = 100 * time.Millisecond
)

var queryChanged = sql.Select(allChangelogCols...).From(tableChangelog)

func (sd spannerDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, sd.config.watchBufferLength)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := timestampFromRevision(afterRevision)

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var err error
			stagedUpdates, currentTxn, err = sd.loadChanges(ctx, currentTxn)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}

			// Write the staged updates to the channel
			for _, changeToWrite := range stagedUpdates {
				select {
				case updates <- changeToWrite:
				default:
					errs <- datastore.NewWatchDisconnectedErr()
					return
				}
			}

			// If there were no changes, sleep a bit
			if len(stagedUpdates) == 0 {
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

func (sd spannerDatastore) loadChanges(
	ctx context.Context,
	afterTimestamp time.Time,
) ([]*datastore.RevisionChanges, time.Time, error) {
	sql, args, err := queryChanged.Where(sq.Gt{colChangeTS: afterTimestamp}).ToSql()
	if err != nil {
		return nil, afterTimestamp, err
	}

	rows := sd.client.Single().Query(ctx, statementFromSQL(sql, args))
	stagedChanges := common.NewChanges()

	newTimestamp := afterTimestamp
	err = rows.Do(func(r *spanner.Row) error {
		tpl := &core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}

		var op int64
		var timestamp time.Time
		var colChangeUUID string
		err := r.Columns(
			&timestamp,
			&colChangeUUID,
			&op,
			&tpl.ResourceAndRelation.Namespace,
			&tpl.ResourceAndRelation.ObjectId,
			&tpl.ResourceAndRelation.Relation,
			&tpl.Subject.Namespace,
			&tpl.Subject.ObjectId,
			&tpl.Subject.Relation,
		)
		if err != nil {
			return err
		}

		newTimestamp = maxTime(newTimestamp, timestamp)

		stagedChanges.AddChange(ctx, revisionFromTimestamp(timestamp), tpl, opMap[op])

		return nil
	})
	if err != nil {
		return nil, afterTimestamp, err
	}

	changes := stagedChanges.AsRevisionChanges()

	return changes, newTimestamp, nil
}

func maxTime(t1 time.Time, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
	return t2
}
