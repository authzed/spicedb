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

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil"
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
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})
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
			datastoreInitFunc: func(t testing.TB, datastore datastore.Datastore) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(t, datastore, `
definition user {}
`, nil)
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
			datastoreInitFunc: func(t testing.TB, datastore datastore.Datastore) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(t, datastore, `
definition user {}
`, nil)
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
			datastoreInitFunc: func(t testing.TB, datastore datastore.Datastore) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(t, datastore, `
caveat is_tuesday(today string) {
   today == 'tuesday'
}
definition user {}
`, nil)
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
			datastoreInitFunc: func(t testing.TB, datastore datastore.Datastore) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(t, datastore, `
definition user {}
definition org {}
`, nil)
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
			datastoreInitFunc: func(t testing.TB, datastore datastore.Datastore) (datastore.Datastore, datastore.Revision) {
				return testfixtures.DatastoreFromSchemaAndTestRelationships(t, datastore, `
definition user {}
definition document {
  relation view: user
}
`, nil)
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
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			conn, _, revision := testserver.NewTestServerWithConfig(t, 0, memdb.DisableGC, true,
				testserver.DefaultTestServerConfig,
				tc.datastoreInitFunc)
			client := v1.NewWatchServiceClient(conn)

			cursor := zedtoken.MustNewFromRevisionForTesting(revision, datalayer.NoSchemaHashInLegacyZedToken)
			if tc.startCursor != nil {
				cursor = tc.startCursor
			}

			ctx, cancel := context.WithCancel(t.Context())
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
					_, err := v1.NewPermissionsServiceClient(conn).WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
						Updates: tc.mutations,
					})
					require.NoError(err)
				}
				if len(tc.mutatedSchema) > 0 {
					_, err := v1.NewSchemaServiceClient(conn).WriteSchema(t.Context(), &v1.WriteSchemaRequest{
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

func TestWatchCarriesSchemaHash(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})
	require := require.New(t)

	// SchemaModeReadNewWriteBoth is required for the datastore to produce real
	// schema hashes; the default legacy mode only yields bypass sentinels.
	config := testserver.DefaultTestServerConfig
	config.DataLayerOpts = []datalayer.DataLayerOption{
		datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteBoth),
	}
	conn, ds, _ := testserver.NewTestServerWithConfig(
		t, 0, memdb.DisableGC, true, config, testfixtures.EmptyDatastore,
	)

	schemaClient := v1.NewSchemaServiceClient(conn)
	permClient := v1.NewPermissionsServiceClient(conn)

	// Write an initial schema; its ZedToken carries the real schema hash.
	writeResp, err := schemaClient.WriteSchema(t.Context(), &v1.WriteSchemaRequest{
		Schema: `definition user {}

definition document {
	relation viewer: user
}`,
	})
	require.NoError(err)

	// The schema hash stored in the datastore at this point.
	headRev, err := ds.HeadRevision(t.Context())
	require.NoError(err)
	require.NotEmpty(headRev.SchemaHash)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Resume the watch from the schema-write ZedToken, which carries the real hash.
	stream, err := v1.NewWatchServiceClient(conn).Watch(ctx, &v1.WatchRequest{
		OptionalUpdateKinds: []v1.WatchKind{
			v1.WatchKind_WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES,
			v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES,
		},
		OptionalStartCursor: writeResp.WrittenAt,
	})
	require.NoError(err)

	watchResponses := make(chan *v1.WatchResponse, 3)
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
						return
					}
					panic(fmt.Errorf("received a stream read error: %w", err))
				}
				watchResponses <- resp
			}
		}
	}()

	// 1) A relationship write does not change the schema, so its ZedToken
	//    carries the real schema hash currently in the datastore.
	_, err = permClient.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1"),
		},
	})
	require.NoError(err)

	// 2) A schema change makes the watch drop the hash: it is no longer tracked
	//    across the change.
	_, err = schemaClient.WriteSchema(t.Context(), &v1.WriteSchemaRequest{
		Schema: `definition user {}

definition organization {}

definition document {
	relation viewer: user
}`,
	})
	require.NoError(err)

	// 3) Any update after the schema change also carries no real hash.
	_, err = permClient.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document2", "viewer", "user", "user1"),
		},
	})
	require.NoError(err)

	received := make([]*v1.WatchResponse, 0, 3)
	for len(received) < 3 {
		select {
		case resp := <-watchResponses:
			received = append(received, resp)
		case <-time.After(3 * time.Second):
			require.FailNow("timed out waiting for watch responses")
		}
	}

	// schemaHashOf decodes the schema hash carried by a watch response's ZedToken.
	schemaHashOf := func(resp *v1.WatchResponse) []byte {
		decoded, err := zedtoken.Decode(resp.GetChangesThrough())
		require.NoError(err)
		return decoded.GetV1().GetSchemaHash()
	}

	// Response 1: a relationship update before any schema change carries the
	// datastore's schema hash for that revision.
	require.NotEmpty(received[0].GetUpdates())
	require.Equal(headRev.SchemaHash, string(schemaHashOf(received[0])),
		"watch ZedToken before a schema change should carry the datastore's schema hash")

	// Response 2: the schema change itself drops the hash.
	require.True(received[1].GetSchemaUpdated())
	require.Empty(schemaHashOf(received[1]),
		"watch ZedToken for a schema change should not carry a schema hash")

	// Response 3: a relationship update after the schema change still has no hash.
	require.NotEmpty(received[2].GetUpdates())
	require.Empty(schemaHashOf(received[2]),
		"watch ZedTokens after a schema change should not carry a schema hash")
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
