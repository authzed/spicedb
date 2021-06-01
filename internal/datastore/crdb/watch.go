package crdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/shopspring/decimal"
)

const queryChangefeed = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', resolved = '1s';"

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, cds.watchBufferLength)
	errors := make(chan error, 1)

	interpolated := fmt.Sprintf(queryChangefeed, tableTuple, afterRevision)

	go func() {
		defer close(updates)
		defer close(errors)

		pendingChanges := make(map[decimal.Decimal][]*pb.RelationTupleUpdate)

		changes, err := cds.conn.Query(ctx, interpolated)
		if err != nil {
			if ctx.Err() == context.Canceled {
				errors <- datastore.ErrWatchCanceled
			} else {
				errors <- err
			}
			return
		}
		defer func() { go changes.Close() }()

		for changes.Next() {
			sqlValues, err := changes.Values()
			if err != nil {
				if ctx.Err() == context.Canceled {
					errors <- datastore.ErrWatchCanceled
				} else {
					errors <- err
				}
				return
			}
			if len(sqlValues) != 3 {
				errors <- fmt.Errorf("wrong number of changefeed values %d != 3", len(sqlValues))
				return
			}

			changeJson, ok := sqlValues[2].([]byte)
			if !ok {
				errors <- fmt.Errorf("wrong type for change JSON, != []byte")
				return
			}

			var changeDetails map[string]interface{}
			if err := json.Unmarshal(changeJson, &changeDetails); err != nil {
				errors <- err
				return
			}

			if resolvedUntyped, ok := changeDetails["resolved"]; ok {
				// This entry indicates that we are ready to potentially emit some changes
				resolvedStr, ok := resolvedUntyped.(string)
				if !ok {
					errors <- fmt.Errorf("resolved value of wrong type: != string")
					return
				}

				resolved, err := decimal.NewFromString(resolvedStr)
				if err != nil {
					errors <- err
					return
				}

				var toEmit []*datastore.RevisionChanges
				for ts, values := range pendingChanges {
					if ts.LessThanOrEqual(resolved) {
						delete(pendingChanges, ts)

						toEmit = append(toEmit, &datastore.RevisionChanges{
							Revision: ts,
							Changes:  values,
						})
					}
				}

				sort.Slice(toEmit, func(i, j int) bool {
					return toEmit[i].Revision.LessThan(toEmit[j].Revision)
				})

				for _, change := range toEmit {
					select {
					case updates <- change:
					default:
						errors <- datastore.ErrWatchDisconnected
						return
					}
				}

				continue
			}

			primaryKeyValuesJson, ok := sqlValues[1].([]byte)
			if !ok {
				errors <- fmt.Errorf("wrong type for PK JSON, %s != []byte", reflect.TypeOf(sqlValues[1]))
				return
			}

			var pkValues []string
			if err := json.Unmarshal(primaryKeyValuesJson, &pkValues); err != nil {
				errors <- err
				return
			}

			if len(pkValues) != 6 {
				errors <- fmt.Errorf("wrong number of pk values %d != 6", len(pkValues))
				return
			}

			afterBlock, hasAfter := changeDetails["after"]
			if !hasAfter {
				errors <- fmt.Errorf("malformed update block, missing 'after' key")
				return
			}

			timestamp, hasTimestamp := changeDetails["updated"]
			if !hasTimestamp {
				errors <- fmt.Errorf("malformed update block, missing 'updated' key")
				return
			}
			tsString, ok := timestamp.(string)
			if !ok {
				errors <- fmt.Errorf("malformed update timestamp: %w", err)
				return
			}

			revision, err := decimal.NewFromString(tsString)
			if err != nil {
				errors <- fmt.Errorf("malformed update timestamp: %w", err)
				return
			}

			oneChange := &pb.RelationTupleUpdate{
				Tuple: &pb.RelationTuple{
					ObjectAndRelation: &pb.ObjectAndRelation{
						Namespace: pkValues[0],
						ObjectId:  pkValues[1],
						Relation:  pkValues[2],
					},
					User: &pb.User{
						UserOneof: &pb.User_Userset{
							Userset: &pb.ObjectAndRelation{
								Namespace: pkValues[3],
								ObjectId:  pkValues[4],
								Relation:  pkValues[5],
							},
						},
					},
				},
			}
			if afterBlock == nil {
				oneChange.Operation = pb.RelationTupleUpdate_DELETE
			} else {
				oneChange.Operation = pb.RelationTupleUpdate_TOUCH
			}

			pendingChanges[revision] = append(pendingChanges[revision], oneChange)
		}
		if changes.Err() != nil {
			if ctx.Err() == context.Canceled {
				errors <- datastore.ErrWatchCanceled
			} else {
				errors <- changes.Err()
			}
			return
		}
	}()
	return updates, errors
}
