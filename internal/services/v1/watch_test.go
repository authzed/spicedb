package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			conn, cleanup, _, revision := testserver.NewTestServer(require, 0, memdb.DisableGC, true, testfixtures.StandardDatastoreWithData)
			t.Cleanup(cleanup)
			client := v1.NewWatchServiceClient(conn)

			cursor := zedtoken.MustNewFromRevision(revision)
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

				_, err := v1.NewPermissionsServiceClient(conn).WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
					Updates: tc.mutations,
				})
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
