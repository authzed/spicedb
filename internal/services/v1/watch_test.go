package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func update(
	op v1.RelationshipUpdate_Operation,
	resourceObjType,
	resourceObjID,
	relation,
	subObjType,
	subObjectID string,
) *v1.RelationshipUpdate {
	return &v1.RelationshipUpdate{
		Operation: op,
		Relationship: &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: resourceObjType,
				ObjectId:   resourceObjID,
			},
			Relation: relation,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: subObjType,
					ObjectId:   subObjectID,
				},
			},
		},
	}
}

func TestWatch(t *testing.T) {
	testCases := []struct {
		name              string
		objectTypesFilter []string
		startCursor       *v1.ZedToken
		mutations         []*v1.RelationshipUpdate
		expectedCode      codes.Code
		expectedUpdates   []*v1.RelationshipUpdate
	}{
		{
			name:         "unfiltered watch",
			expectedCode: codes.OK,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "folder", "folder2", "viewer", "user", "user1"),
			},
			expectedUpdates: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "folder", "folder2", "viewer", "user", "user1"),
			},
		},
		{
			name:              "watch with objectType filter",
			expectedCode:      codes.OK,
			objectTypesFilter: []string{"document"},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
			},
			expectedUpdates: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
			},
		},
		{
			name:         "invalid zedtoken",
			startCursor:  &v1.ZedToken{Token: "bad-token"},
			expectedCode: codes.InvalidArgument,
		},
		{
			name:         "empty zedtoken fails validation",
			startCursor:  &v1.ZedToken{Token: ""},
			expectedCode: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
			require.True(revision.GreaterThan(decimal.Zero))

			client, stop := newWatchServicer(require, ds)
			defer stop()

			cursor := zedtoken.NewFromRevision(revision)
			if tc.startCursor != nil {
				cursor = tc.startCursor
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream, err := client.Watch(ctx, &v1.WatchRequest{
				OptionalObjectTypes: tc.objectTypesFilter,
				OptionalStartCursor: cursor,
			})
			require.NoError(err)

			if tc.expectedCode == codes.OK {
				updatesChan := make(chan []*v1.RelationshipUpdate, len(tc.mutations))

				go func() {
					defer close(updatesChan)

					for {
						select {
						case <-ctx.Done():
							return
						case <-time.After(3 * time.Second):
							panic(fmt.Errorf("timed out waiting for stream updates"))
						default:
							resp, err := stream.Recv()
							if err != nil {
								errStatus, ok := status.FromError(err)
								if (ok && (errStatus.Code() == codes.Canceled || errStatus.Code() == codes.Unavailable)) || errors.Is(err, io.EOF) {
									break
								}

								panic(fmt.Errorf("received a stream read error: %w", err))
							}

							updatesChan <- resp.Updates
						}
					}
				}()

				_, err = ds.WriteTuples(context.Background(), nil, tc.mutations)
				require.NoError(err)

				var receivedUpdates []*v1.RelationshipUpdate

				for len(receivedUpdates) < len(tc.expectedUpdates) {
					select {
					case updates := <-updatesChan:
						receivedUpdates = append(receivedUpdates, updates...)
					case <-time.After(1 * time.Second):
						require.FailNow("timed out waiting for updates")
						return
					}
				}

				require.Equal(sortUpdates(tc.expectedUpdates), sortUpdates(receivedUpdates))
			} else {
				_, err := stream.Recv()
				grpcutil.RequireStatus(t, tc.expectedCode, err)
			}
		})
	}
}

func newWatchServicer(
	require *require.Assertions,
	ds datastore.Datastore,
) (v1.WatchServiceClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	s := testfixtures.NewTestServer()

	v1.RegisterWatchServiceServer(s, NewWatchServer(ds))
	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()

	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(err)

	return v1.NewWatchServiceClient(conn), func() {
		require.NoError(conn.Close())
		s.Stop()
		require.NoError(lis.Close())
	}
}

func sortUpdates(in []*v1.RelationshipUpdate) []*v1.RelationshipUpdate {
	out := make([]*v1.RelationshipUpdate, 0, len(in))
	out = append(out, in...)
	sort.Slice(out, func(i, j int) bool {
		left, right := out[i], out[j]
		compareResult := strings.Compare(tuple.MustRelString(left.Relationship), tuple.MustRelString(right.Relationship))
		if compareResult < 0 {
			return true
		}
		if compareResult > 0 {
			return false
		}

		return left.Operation < right.Operation
	})

	return out
}
