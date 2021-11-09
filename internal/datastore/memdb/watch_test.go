package memdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestWatch(t *testing.T) {
	require := require.New(t)

	memdbStore, err := NewMemdbDatastore(0, 0, DisableGC, 0)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithSchema(memdbStore, require)
	require.True(revision.GreaterThan(decimal.Zero))

	receivedChangesChan := make(chan *datastore.RevisionChanges)

	changes, errchan := ds.Watch(context.Background(), revision)
	go func() {
		for {
			select {
			case change, ok := <-changes:
				if ok {
					receivedChangesChan <- change
				}
			case err = <-errchan:
				require.FailNow("received unexpected error: %v", err)
			}
		}
	}()

	for i := 0; i < 3; i++ {
		update := &v1.RelationshipUpdate{
			Operation: v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: &v1.Relationship{
				Resource: &v1.ObjectReference{
					ObjectType: "document",
					ObjectId:   fmt.Sprintf("document%d", i),
				},
				Relation: "viewer",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "user1",
					},
				},
			},
		}

		revision, err = ds.WriteTuples(context.Background(), nil, []*v1.RelationshipUpdate{update})
		require.True(revision.GreaterThan(decimal.Zero))
		require.NoError(err)

		ready := make(chan struct{})

		var receivedChanges []*datastore.RevisionChanges
		go func() {
			defer func() {
				ready <- struct{}{}
			}()

			select {
			case change := <-receivedChangesChan:
				receivedChanges = append(receivedChanges, change)
			case <-time.After(3 * time.Second):
				require.FailNow("timed out waiting for watch updates")
				return
			}
		}()

		<-ready

		require.Equal(1, len(receivedChanges))
	}
}
