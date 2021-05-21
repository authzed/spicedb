package crdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/shopspring/decimal"
)

const queryChangefeed = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s';"

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, cds.watchBufferLength)
	errors := make(chan error, 1)

	interpolated := fmt.Sprintf(queryChangefeed, tableTuple, afterRevision)

	go func() {
		defer close(updates)
		defer close(errors)

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
			var tableName string
			var primaryKeyValuesJson, changeJson []byte
			if err := changes.Scan(&tableName, &primaryKeyValuesJson, &changeJson); err != nil {
				if ctx.Err() == context.Canceled {
					errors <- datastore.ErrWatchCanceled
				} else {
					errors <- err
				}
				return
			}

			var pkValues []string
			if err := json.Unmarshal(primaryKeyValuesJson, &pkValues); err != nil {
				errors <- err
				return
			}

			fmt.Printf("\n\n%#v\n\n", pkValues)

			if len(pkValues) != 6 {
				errors <- fmt.Errorf("wrong number of pk values %d != 6", len(pkValues))
			}

			var changeDetails map[string]interface{}
			if err := json.Unmarshal(changeJson, &changeDetails); err != nil {
				errors <- err
				return
			}

			fmt.Printf("\n\n%#v\n\n", changeDetails)

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

			change := &datastore.RevisionChanges{
				Revision: revision,
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

			change.Changes = append(change.Changes, oneChange)

			select {
			case updates <- change:
			default:
				errors <- datastore.ErrWatchDisconnected
				return
			}
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
