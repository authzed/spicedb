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
	"github.com/authzed/spicedb/pkg/datastore"
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
		name                   string
		watchKinds             []v1.WatchKind
		datastoreInitFunc      testserver.DatastoreInitFunc
		startCursor            *v1.ZedToken
		expectedWatchResponses []*v1.WatchResponse
		expectedCode           codes.Code
		// for relationship updates
		objectTypesFilter   []string
		relationshipFilters []*v1.RelationshipFilter
		mutations           []*v1.RelationshipUpdate
		// for schema updates
		mutatedSchema string
	}{
		{
			name:              "unfiltered watch",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			expectedCode:      codes.OK,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "folder", "folder2", "viewer", "user", "user1"),
			},
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
					update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "folder", "folder2", "viewer", "user", "user1"),
				}},
			},
		},
		{
			name:              "watch with objectType filter",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			expectedCode:      codes.OK,
			objectTypesFilter: []string{"document"},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
			},
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				}},
			},
		},
		{
			name:              "watch with relationship filters",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			expectedCode:      codes.OK,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					ResourceType: "document",
				},
				{
					OptionalResourceIdPrefix: "d",
				},
			},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
			},
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				}},
			},
		},
		{
			name:              "watch with modified relationship filters",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			expectedCode:      codes.OK,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					ResourceType: "folder",
				},
			},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
			},
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
				}},
			},
		},
		{
			name:              "watch with resource ID prefix",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			expectedCode:      codes.OK,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					OptionalResourceIdPrefix: "document1",
				},
			},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
			},
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
				}},
			},
		},
		{
			name:              "watch with shorter resource ID prefix",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			expectedCode:      codes.OK,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					OptionalResourceIdPrefix: "doc",
				},
			},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "auditors", "viewer", "user", "auditor"),
			},
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "viewer", "user", "user1"),
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document2", "viewer", "user", "user1"),
				}},
			},
		},
		{
			name:              "invalid zedtoken",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			startCursor:       &v1.ZedToken{Token: "bad-token"},
			expectedCode:      codes.InvalidArgument,
		},
		{
			name:              "empty zedtoken fails validation",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			startCursor:       &v1.ZedToken{Token: ""},
			expectedCode:      codes.InvalidArgument,
		},
		{
			name:              "watch with both kinds of filters",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					OptionalResourceIdPrefix: "doc",
				},
			},
			objectTypesFilter: []string{"document"},
			expectedCode:      codes.InvalidArgument,
		},
		{
			name:              "watch with both fields of filter",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					OptionalResourceIdPrefix: "doc",
					OptionalResourceId:       "document1",
				},
			},
			expectedCode: codes.InvalidArgument,
		},
		{
			name:              "watch with invalid filter resource type",
			watchKinds:        []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			datastoreInitFunc: testfixtures.StandardDatastoreWithData,
			relationshipFilters: []*v1.RelationshipFilter{
				{
					ResourceType: "invalid",
				},
			},
			expectedCode: codes.FailedPrecondition,
		},
		{
			name:       "watch with schema kind returns a schema update (new definition)",
			watchKinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES},
			datastoreInitFunc: func(datastore datastore.Datastore, _ *require.Assertions) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(datastore, `
definition user {}
`, nil, require.New(t))
			},
			mutatedSchema: `definition user {}
definition org {}`,
			expectedWatchResponses: []*v1.WatchResponse{
				{SchemaUpdated: true},
			},
		},
		{
			name:       "watch with schema kind returns a schema update (new caveat)",
			watchKinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES},
			datastoreInitFunc: func(datastore datastore.Datastore, _ *require.Assertions) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(datastore, `
definition user {}
`, nil, require.New(t))
			},
			mutatedSchema: `
caveat is_tuesday(today string) {
   today == 'tuesday'
}
definition user {}
`,
			expectedWatchResponses: []*v1.WatchResponse{
				{SchemaUpdated: true},
			},
		},
		{
			name:       "watch with schema kind returns a schema update (deleted caveat)",
			watchKinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES},
			datastoreInitFunc: func(datastore datastore.Datastore, _ *require.Assertions) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(datastore, `
caveat is_tuesday(today string) {
   today == 'tuesday'
}
definition user {}
`, nil, require.New(t))
			},
			mutatedSchema: `
definition user {}
`,
			expectedWatchResponses: []*v1.WatchResponse{
				{SchemaUpdated: true},
			},
		},
		{
			name:       "watch with schema kind returns a schema update (deleted namespace)",
			watchKinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES},
			datastoreInitFunc: func(datastore datastore.Datastore, _ *require.Assertions) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(datastore, `
definition user {}
definition org {}
`, nil, require.New(t))
			},
			mutatedSchema: `
definition user {}
`,
			expectedWatchResponses: []*v1.WatchResponse{
				{SchemaUpdated: true},
			},
		},
		{
			name: "watch with all kinds",
			watchKinds: []v1.WatchKind{
				v1.WatchKind_WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES,
				v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES,
				v1.WatchKind_WATCH_KIND_INCLUDE_CHECKPOINTS,
			},
			datastoreInitFunc: func(datastore datastore.Datastore, _ *require.Assertions) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(datastore, `
definition user {}
definition document {
  relation view: user
}
`, nil, require.New(t))
			},
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "view", "user", "user1"),
			},
			mutatedSchema: `
definition new {}
definition user {}
definition document {
  relation view: user
}
`,
			expectedWatchResponses: []*v1.WatchResponse{
				{Updates: []*v1.RelationshipUpdate{
					update(v1.RelationshipUpdate_OPERATION_TOUCH, "document", "document1", "view", "user", "user1"),
				}},
				{IsCheckpoint: true},
				{SchemaUpdated: true},
				{IsCheckpoint: true},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			conn, cleanup, _, revision := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tc.datastoreInitFunc)
			t.Cleanup(cleanup)
			client := v1.NewWatchServiceClient(conn)

			cursor := zedtoken.MustNewFromRevision(revision)
			if tc.startCursor != nil {
				cursor = tc.startCursor
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream, err := client.Watch(ctx, &v1.WatchRequest{
				OptionalObjectTypes:         tc.objectTypesFilter,
				OptionalRelationshipFilters: tc.relationshipFilters,
				OptionalStartCursor:         cursor,
				OptionalUpdateKinds:         tc.watchKinds,
			})
			require.NoError(err)

			if tc.expectedCode == codes.OK {
				watchResponses := make(chan *v1.WatchResponse, 1)

				go func() {
					defer close(watchResponses)

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

							watchResponses <- resp
						}
					}
				}()

				if len(tc.mutations) > 0 {
					_, err := v1.NewPermissionsServiceClient(conn).WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
						Updates: tc.mutations,
					})
					require.NoError(err)
				}
				if len(tc.mutatedSchema) > 0 {
					_, err := v1.NewSchemaServiceClient(conn).WriteSchema(context.Background(), &v1.WriteSchemaRequest{
						Schema: tc.mutatedSchema,
					})
					require.NoError(err)
				}

				var received []*v1.WatchResponse

				for len(received) < len(tc.expectedWatchResponses) {
					select {
					case receivedWatchResponse := <-watchResponses:
						received = append(received, receivedWatchResponse)
					case <-time.After(1 * time.Second):
						require.FailNow("timed out waiting for message")
						return
					}
				}

				require.Len(received, len(tc.expectedWatchResponses))

				for i, expectedWatchResponse := range tc.expectedWatchResponses {
					require.Equal(sortUpdates(expectedWatchResponse.Updates), sortUpdates(received[i].GetUpdates()))
					require.Equal(expectedWatchResponse.SchemaUpdated, received[i].GetSchemaUpdated())
					require.Equal(expectedWatchResponse.IsCheckpoint, received[i].GetIsCheckpoint())
				}
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
		compareResult := strings.Compare(tuple.MustV1RelString(left.Relationship), tuple.MustV1RelString(right.Relationship))
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
