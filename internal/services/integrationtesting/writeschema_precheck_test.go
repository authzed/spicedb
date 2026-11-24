//go:build datastore

package integrationtesting_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestWriteSchemaCatchesRelationshipWrittenWhileWaitingForLock checks that a schema write rejects
// a relationship under a relation it removes, when the relationship is committed after the
// schema write's checks at a snapshot passed but before it took the schema lock.
//
// A relationship write takes the schema lock (FOR SHARE), writes the relationship, and waits. The
// schema write runs its checks at a snapshot, which pass because the relationship is not
// committed, and then waits for the lock (FOR UPDATE). The relationship write then commits, and
// the schema write must find the relationship under the lock.
func TestWriteSchemaCatchesRelationshipWrittenWhileWaitingForLock(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		name := "serializable"
		if relaxed {
			name = "repeatable-read"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()

			b := testdatastore.RunDatastoreEngine(t, "postgres")
			var uri string
			initFunc := config.DatastoreConfigInitFunc(t,
				dsconfig.WithRelaxedIsolationLevel(relaxed),
				dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
			)
			ds := b.NewDatastore(t, func(engine, u string) datastore.Datastore {
				uri = u
				return initFunc(engine, u)
			})

			conns := testserver.TestClusterWithDispatch(t, 1, ds, server.WithExperimentalSchemaMode("read-new-write-new"))
			schemaClient := v1.NewSchemaServiceClient(conns[0])

			_, err := schemaClient.WriteSchema(ctx, &v1.WriteSchemaRequest{
				Schema: "definition user {}\ndefinition document {\n\trelation viewer: user\n\trelation editor: user\n}",
			})
			require.NoError(t, err)

			head, err := ds.HeadRevision(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, head.SchemaHash)

			// The relationship write: hold FOR SHARE with the relationship written but not
			// committed.
			relWriteInside := make(chan struct{})
			releaseRelWrite := make(chan struct{})
			relWriteDone := make(chan error, 1)
			go func() {
				_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					if err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
						tuple.Create(tuple.MustParse("document:doc1#editor@user:tom")),
					}); err != nil {
						return err
					}
					close(relWriteInside)
					<-releaseRelWrite
					return nil
				}, options.WithSchemaHashPrecondition(head.SchemaHash), options.WithDisableRetries(true))
				relWriteDone <- err
			}()
			<-relWriteInside

			// The schema write removes the relation.
			schemaWriteDone := make(chan error, 1)
			go func() {
				_, err := schemaClient.WriteSchema(ctx, &v1.WriteSchemaRequest{
					Schema: "definition user {}\ndefinition document {\n\trelation viewer: user\n}",
				})
				schemaWriteDone <- err
			}()

			// Wait for the schema write to be waiting for the lock, which is after its checks at a
			// snapshot.
			monitor, err := pgx.Connect(ctx, uri)
			require.NoError(t, err)
			t.Cleanup(func() { _ = monitor.Close(context.Background()) })
			require.Eventually(t, func() bool {
				var waiting int
				require.NoError(t, monitor.QueryRow(ctx, `
					SELECT count(*) FROM pg_stat_activity
					WHERE wait_event_type = 'Lock' AND query LIKE '%schema_revision%FOR UPDATE%'`).Scan(&waiting))
				return waiting > 0
			}, 10*time.Second, 10*time.Millisecond, "the schema write did not wait for the schema lock")

			close(releaseRelWrite)
			require.NoError(t, <-relWriteDone)

			select {
			case err := <-schemaWriteDone:
				require.Error(t, err, "the schema write removed a relation with a relationship under it")
				require.Equal(t, codes.InvalidArgument, status.Code(err), err.Error())
				require.Contains(t, err.Error(), "document:doc1#editor@user:tom")
			case <-time.After(20 * time.Second):
				t.Fatal("the schema write did not finish")
			}
		})
	}
}
