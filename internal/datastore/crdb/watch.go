package crdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/authzed/spicedb/internal/datastore/common"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const queryChangefeed = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', resolved = '1s';"

type changeDetails struct {
	Resolved string
	Updated  string
	After    *struct {
		CaveatContext map[string]any `json:"caveat_context"`
		CaveatName    string         `json:"caveat_name"`
	}
}

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, cds.watchBufferLength)
	errs := make(chan error, 1)

	interpolated := fmt.Sprintf(queryChangefeed, tableTuple, afterRevision)

	go func() {
		defer close(updates)
		defer close(errs)

		pendingChanges := make(map[string]*datastore.RevisionChanges)

		changes, err := cds.pool.Query(ctx, interpolated)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				errs <- datastore.NewWatchCanceledErr()
			} else {
				errs <- err
			}
			return
		}

		// We call Close async here because it can be slow and blocks closing the channels. There is
		// no return value so we're not really losing anything.
		defer func() { go changes.Close() }()

		for changes.Next() {
			var unused interface{}
			var changeJSON []byte
			var primaryKeyValuesJSON []byte

			if err := changes.Scan(&unused, &primaryKeyValuesJSON, &changeJSON); err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}

			var details changeDetails
			if err := json.Unmarshal(changeJSON, &details); err != nil {
				errs <- err
				return
			}

			if details.Resolved != "" {
				// This entry indicates that we are ready to potentially emit some changes
				resolved, err := cds.RevisionFromString(details.Resolved)
				if err != nil {
					errs <- err
					return
				}

				var toEmit []*datastore.RevisionChanges
				for ts, values := range pendingChanges {
					if resolved.GreaterThan(values.Revision) {
						delete(pendingChanges, ts)

						toEmit = append(toEmit, values)
					}
				}

				sort.Slice(toEmit, func(i, j int) bool {
					return toEmit[i].Revision.LessThan(toEmit[j].Revision)
				})

				for _, change := range toEmit {
					select {
					case updates <- change:
					default:
						errs <- datastore.NewWatchDisconnectedErr()
						return
					}
				}

				continue
			}

			var pkValues [6]string
			if err := json.Unmarshal(primaryKeyValuesJSON, &pkValues); err != nil {
				errs <- err
				return
			}

			revision, err := cds.RevisionFromString(details.Updated)
			if err != nil {
				errs <- fmt.Errorf("malformed update timestamp: %w", err)
				return
			}

			var caveatName string
			var caveatContext map[string]any
			if details.After != nil && details.After.CaveatName != "" {
				caveatName = details.After.CaveatName
				caveatContext = details.After.CaveatContext
			}
			ctxCaveat, err := common.ContextualizedCaveatFrom(caveatName, caveatContext)
			if err != nil {
				errs <- err
				return
			}

			oneChange := &core.RelationTupleUpdate{
				Tuple: &core.RelationTuple{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: pkValues[0],
						ObjectId:  pkValues[1],
						Relation:  pkValues[2],
					},
					Subject: &core.ObjectAndRelation{
						Namespace: pkValues[3],
						ObjectId:  pkValues[4],
						Relation:  pkValues[5],
					},
					Caveat: ctxCaveat,
				},
			}

			if details.After == nil {
				oneChange.Operation = core.RelationTupleUpdate_DELETE
			} else {
				oneChange.Operation = core.RelationTupleUpdate_TOUCH
			}

			pending, ok := pendingChanges[details.Updated]
			if !ok {
				pending = &datastore.RevisionChanges{
					Revision: revision,
				}
				pendingChanges[details.Updated] = pending
			}
			pending.Changes = append(pending.Changes, oneChange)
		}
		if changes.Err() != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				errs <- datastore.NewWatchCanceledErr()
			} else {
				errs <- changes.Err()
			}
			return
		}
	}()
	return updates, errs
}
