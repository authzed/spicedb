//go:build datastore

package spanner

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSpannerBulkLoad(t *testing.T) {
	b := testdatastore.RunSpannerForTesting(t)
	for _, tc := range []struct {
		name string
		run  datastoreTestFunc
	}{
		{"Atomicity", bulkLoadAtomicityTest},
		{"MixedWrites", bulkLoadMixedWritesTest},
		{"SnapshotsAndWatch", bulkLoadSnapshotsAndWatchTest},
		{"RetriesEnabled", func(t *testing.T, ds datastore.Datastore) {
			bulkLoadRetryBehaviorTest(t, ds, false)
		}},
		{"DisabledRetries", func(t *testing.T, ds datastore.Datastore) {
			bulkLoadRetryBehaviorTest(t, ds, true)
		}},
	} {
		t.Run(tc.name, createDatastoreTest(b, func(t *testing.T, ds datastore.Datastore) {
			ds, _ = testfixtures.StandardDatastoreWithSchema(t, ds)
			tc.run(t, ds)
		}))
	}
}

func bulkSourceForTest(t *testing.T, next func(context.Context) (*tuple.Relationship, error)) datastore.BulkWriteRelationshipSource {
	source := mocks.NewMockBulkWriteRelationshipSource(gomock.NewController(t))
	source.EXPECT().Next(gomock.Any()).DoAndReturn(next).AnyTimes()
	return source
}

func bulkRelationshipsForTest(t *testing.T, rels ...tuple.Relationship) datastore.BulkWriteRelationshipSource {
	i := 0
	return bulkSourceForTest(t, func(context.Context) (*tuple.Relationship, error) {
		if i == len(rels) {
			return nil, nil
		}
		rel := &rels[i]
		i++
		return rel, nil
	})
}

func readBulkRelationships(t *testing.T, ds datastore.Datastore) []tuple.Relationship {
	head, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	iter, err := ds.SnapshotReader(head.Revision).QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)
	var result []tuple.Relationship
	for rel, err := range iter {
		require.NoError(t, err)
		rel.OptionalCaveat = rel.OptionalCaveat.CloneVT()
		if rel.OptionalExpiration != nil {
			expiration := *rel.OptionalExpiration
			rel.OptionalExpiration = &expiration
		}
		result = append(result, rel)
	}
	return result
}

func bulkLoadAtomicityTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	forced := errors.New("abort the bulk transaction")
	rel := tuple.MustParse("document:rollback#viewer@user:tom")
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, bulkRelationshipsForTest(t, rel))
		require.NoError(t, err)
		iter, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: testfixtures.DocumentNS.Name,
		}, options.WithQueryShape(queryshape.FindResourceOfType))
		require.NoError(t, err)
		(testfixtures.RelationshipChecker{Require: require.New(t), DS: ds}).VerifyIteratorCount(iter, 0)
		return forced
	})
	require.ErrorIs(t, err, forced)
	require.Empty(t, readBulkRelationships(t, ds))

	next := 0
	source := bulkSourceForTest(t, func(context.Context) (*tuple.Relationship, error) {
		next++
		if next == 1 {
			return &rel, nil
		}
		return nil, forced
	})
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, source)
		return err
	})
	require.ErrorIs(t, err, forced)
	require.Empty(t, readBulkRelationships(t, ds))

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Create(rel)})
	})
	require.NoError(t, err)
	batch := make([]tuple.Relationship, 0, 101)
	for i := range 100 {
		batch = append(batch, tuple.MustParse(fmt.Sprintf("document:new%d#viewer@user:tom", i)))
	}
	batch = append(batch, rel) // Fail after the first INSERT statement succeeded.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, bulkRelationshipsForTest(t, batch...))
		return err
	}, options.WithDisableRetries(true))
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(t, []tuple.Relationship{rel}, readBulkRelationships(t, ds))
}

func bulkLoadMixedWritesTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	for _, limited := range []bool{false, true} {
		rel := tuple.MustParse(fmt.Sprintf("document:replace-%v#viewer@user:tom", limited))
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Create(rel)})
		})
		require.NoError(t, err)
		_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			if limited {
				limit := uint64(1)
				_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
					ResourceType: rel.Resource.ObjectType, OptionalResourceId: rel.Resource.ObjectID,
				}, options.WithDeleteLimit(&limit))
				if err != nil {
					return err
				}
			} else if err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(rel)}); err != nil {
				return err
			}
			_, err := rwt.BulkLoad(ctx, bulkRelationshipsForTest(t, rel))
			return err
		})
		require.NoError(t, err)
	}

	rel := tuple.MustParse("document:bulk-touch#viewer@user:tom")
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, bulkRelationshipsForTest(t, rel))
		if err != nil {
			return err
		}
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
	})
	require.NoError(t, err)
	require.Len(t, readBulkRelationships(t, ds), 3)

	deleted := tuple.MustParse("document:bulk-delete#viewer@user:tom")
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, bulkRelationshipsForTest(t, deleted))
		if err != nil {
			return err
		}
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(deleted)})
	})
	require.NoError(t, err)
	require.Len(t, readBulkRelationships(t, ds), 3)
}

func bulkLoadSnapshotsAndWatchTest(t *testing.T, ds datastore.Datastore) {
	ctx := t.Context()
	head, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	changes, errs := ds.Watch(watchCtx, head.Revision, datastore.WatchOptions{Content: datastore.WatchRelationships})

	base := time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour)
	expiration := base
	current := tuple.MustParse("document:reuse0#caveated_viewer@user:tom")
	current.OptionalExpiration = &expiration
	current.OptionalCaveat = &core.ContextualizedCaveat{CaveatName: "somecaveat", Context: &structpb.Struct{
		Fields: map[string]*structpb.Value{},
	}}
	i := 0
	source := bulkSourceForTest(t, func(context.Context) (*tuple.Relationship, error) {
		if i == 2 {
			return nil, nil
		}
		current.Resource.ObjectID = "reuse" + strconv.Itoa(i)
		current.OptionalCaveat.Context.Fields["secret"] = structpb.NewStringValue(strconv.Itoa(i))
		expiration = base.Add(time.Duration(i) * time.Minute)
		i++
		return &current, nil
	})
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, source)
		if err != nil {
			return err
		}
		_, err = rwt.BulkLoad(ctx, bulkRelationshipsForTest(t, tuple.MustParse("document:second-call#viewer@user:tom")))
		return err
	})
	require.NoError(t, err)
	require.Len(t, readBulkRelationships(t, ds), 3)
	for _, rel := range readBulkRelationships(t, ds) {
		if !strings.HasPrefix(rel.Resource.ObjectID, "reuse") {
			continue
		}
		i, err := strconv.Atoi(strings.TrimPrefix(rel.Resource.ObjectID, "reuse"))
		require.NoError(t, err)
		require.Equal(t, strconv.Itoa(i), rel.OptionalCaveat.Context.AsMap()["secret"])
		require.True(t, rel.OptionalExpiration.Equal(base.Add(time.Duration(i)*time.Minute)))
	}
	iter := datastore.UnwrapAs[*spannerDatastore](ds).client.Single().Read(ctx, tableRelationship, spanner.AllKeys(), []string{colTimestamp})
	defer iter.Stop()
	require.NoError(t, iter.Do(func(row *spanner.Row) error {
		var timestamp time.Time
		require.NoError(t, row.Column(0, &timestamp))
		require.True(t, timestamp.Equal(revision.(revisions.TimestampRevision).Time()))
		return nil
	}))
	select {
	case change := <-changes:
		require.True(t, revision.Equal(change.Revision))
		require.Len(t, change.RelationshipChanges, 3)
		expected := map[string]tuple.Relationship{}
		for _, rel := range readBulkRelationships(t, ds) {
			expected[rel.Resource.ObjectID] = rel
		}
		seen := map[string]bool{}
		for _, update := range change.RelationshipChanges {
			id := update.Relationship.Resource.ObjectID
			require.Contains(t, expected, id)
			require.False(t, seen[id])
			seen[id] = true
			require.Equal(t, tuple.UpdateOperationTouch, update.Operation)
			require.Equal(t, expected[id].RelationshipReference, update.Relationship.RelationshipReference)
			if expected[id].OptionalCaveat == nil {
				require.Nil(t, update.Relationship.OptionalCaveat)
			} else {
				require.NotNil(t, update.Relationship.OptionalCaveat)
				require.Equal(t, expected[id].OptionalCaveat.CaveatName, update.Relationship.OptionalCaveat.CaveatName)
				require.Equal(t, expected[id].OptionalCaveat.Context.AsMap(), update.Relationship.OptionalCaveat.Context.AsMap())
			}
		}
	case err := <-errs:
		t.Fatalf("watch failed: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for bulk changes")
	}
}

func bulkLoadRetryBehaviorTest(t *testing.T, ds datastore.Datastore, disableRetries bool) {
	phases := []string{"INSERT", "Commit"}
	if !disableRetries {
		phases = []string{"Commit"}
	}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			sd := datastore.UnwrapAs[*spannerDatastore](ds)
			original := sd.client
			var injected atomic.Bool
			client, err := spanner.NewClient(t.Context(), sd.database, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
				func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
					inject := phase == "Commit" && method == "/google.spanner.v1.Spanner/Commit"
					if request, ok := req.(*sppb.ExecuteSqlRequest); ok && phase == "INSERT" {
						inject = strings.HasPrefix(request.Sql, "INSERT ")
					}
					if inject && !injected.Swap(true) {
						if request, ok := req.(*sppb.CommitRequest); ok {
							// A real server abort releases the transaction's DML locks.
							err := invoke(ctx, "/google.spanner.v1.Spanner/Rollback", &sppb.RollbackRequest{
								Session: request.Session, TransactionId: request.GetTransactionId(),
							}, &emptypb.Empty{}, cc, opts...)
							if err != nil {
								return err
							}
						}
						return status.Error(codes.Aborted, "forced bulk transaction abort")
					}
					return invoke(ctx, method, req, reply, cc, opts...)
				},
			)))
			require.NoError(t, err)
			sd.client = client
			defer func() {
				sd.client = original
				client.Close()
			}()
			source := bulkRelationshipsForTest(t, tuple.MustParse("document:no-retry#viewer@user:tom"))
			calls := 0
			_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				calls++
				if !disableRetries {
					source = bulkRelationshipsForTest(t, tuple.MustParse("document:no-retry#viewer@user:tom"))
				}
				loaded, err := rwt.BulkLoad(ctx, source)
				if err == nil {
					require.Equal(t, uint64(1), loaded)
				}
				return err
			}, options.WithDisableRetries(disableRetries))
			require.True(t, injected.Load())
			if disableRetries {
				require.Error(t, err)
				require.Equal(t, 1, calls)
				require.Empty(t, readBulkRelationships(t, ds))
			} else {
				require.NoError(t, err)
				require.GreaterOrEqual(t, calls, 2)
				require.Len(t, readBulkRelationships(t, ds), 1)
			}
		})
	}
}
