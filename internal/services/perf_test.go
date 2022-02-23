package services_test

import (
	"context"
	"sync"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestBurst(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)
	ds, _ = tf.StandardDatastoreWithData(ds, require.New(t))

	conns, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
	t.Cleanup(cleanup)

	zerolog.SetGlobalLevel(zerolog.Disabled)
	client := v1.NewPermissionsServiceClient(conns[0])
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		rel := tuple.MustToRelationship(tuple.Parse(tf.StandardTuples[i%(len(tf.StandardTuples))]))
		run := make(chan struct{})
		wg.Add(1)
		go func() {
			<-run
			defer wg.Done()
			_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
				Updates: []*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: rel,
				}},
			})
			require.NoError(t, err)
			_, err = client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
				Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: "masterplan"},
				Permission: "viewer",
				Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "eng_lead"}},
			})
			require.NoError(t, err)
		}()
		run <- struct{}{}
	}
	wg.Wait()
}
