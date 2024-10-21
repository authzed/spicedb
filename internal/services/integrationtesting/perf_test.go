//go:build ci && docker && !skipintegrationtests
// +build ci,docker,!skipintegrationtests

package integrationtesting_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/spanner"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestBurst(t *testing.T) {
	blacklist := []string{
		spanner.Engine, // spanner emulator doesn't support parallel transactions
	}

	for _, engine := range datastore.Engines {
		if slices.Contains(blacklist, engine) {
			continue
		}
		b := testdatastore.RunDatastoreEngine(t, engine)
		t.Run(engine, func(t *testing.T) {
			ds := b.NewDatastore(t, config.DatastoreConfigInitFunc(t,
				dsconfig.WithWatchBufferLength(0),
				dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
				dsconfig.WithRevisionQuantization(10),
				dsconfig.WithMaxRetries(50),
				dsconfig.WithRequestHedgingEnabled(false)))
			ds, revision := tf.StandardDatastoreWithData(ds, require.New(t))

			conns, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
			t.Cleanup(cleanup)

			client := v1.NewPermissionsServiceClient(conns[0])
			var wg sync.WaitGroup
			for i := 0; i < 100; i++ {
				rel := tuple.ToV1Relationship(tuple.MustParse(tf.StandardRelationships[i%(len(tf.StandardRelationships))]))
				run := make(chan struct{})
				wg.Add(1)
				go func() {
					<-run
					defer wg.Done()
					_, err := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
							},
						},
						Resource:   rel.Resource,
						Permission: "viewer",
						Subject:    rel.Subject,
					})
					require.NoError(t, err)
				}()
				run <- struct{}{}
			}
			wg.Wait()
		})
	}
}
