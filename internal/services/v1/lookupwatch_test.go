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

	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	lookupwatchv1 "github.com/authzed/spicedb/pkg/proto/lookupwatch/v1"
	"github.com/authzed/spicedb/pkg/zedtoken"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func permissionUpdate(
	resourceType string,
	resourceId string,
	subjectType string,
	subjectId string,
	optionalSubjectRelation string,
	permission v1.CheckPermissionResponse_Permissionship,
) *lookupwatchv1.PermissionUpdate {
	return &lookupwatchv1.PermissionUpdate{
		Resource: &v1.ObjectReference{
			ObjectType: resourceType,
			ObjectId:   resourceId,
		},
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subjectType,
				ObjectId:   subjectId,
			},
			OptionalRelation: optionalSubjectRelation,
		},
		UpdatedPermission: permission,
	}
}

type DataStoreType int64

const (
	Standard DataStoreType = iota
	Custom
)

func TestLookupWatch(t *testing.T) {
	testCases := []struct {
		name                    string
		resourceObjectType      string
		permission              string
		subjectObjectType       string
		OptionalSubjectRelation string
		dsInitFunc              func(datastore.Datastore, *require.Assertions) (datastore.Datastore, datastore.Revision)
		startCursor             *v1.ZedToken
		mutations               []*v1.RelationshipUpdate
		expectedCode            codes.Code
		expectedUpdates         []*lookupwatchv1.PermissionUpdate
	}{
		{
			name:                    "basic test on create relation",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			dsInitFunc:              testfixtures.StandardDatastoreWithData,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("document", "document1", "user", "user1", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
			},
		},
		{
			name:                    "delete an intermediate relation",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			dsInitFunc:              testfixtures.StandardDatastoreWithData,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "company", "viewer", "folder", "auditors", "viewer"),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("document", "companyplan", "user", "auditor", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION),
				permissionUpdate("document", "masterplan", "user", "auditor", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION),
			},
		},
		{
			name:                    "change with no impact on the watched resourceType",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			dsInitFunc:              testfixtures.StandardDatastoreWithData,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "folder", "company", "parent", "folder", "aNewFolder", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{},
		},
		{
			name:                    "adding an intermediate relation",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			dsInitFunc:              testfixtures.StandardDatastoreWithData,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "folder", "new_folder", "parent", "folder", "company", ""),
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "new_document", "parent", "folder", "new_folder", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("document", "new_document", "user", "legal", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
				permissionUpdate("document", "new_document", "user", "owner", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
				permissionUpdate("document", "new_document", "user", "auditor", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
			},
		},
		{
			name:                    "ensure LS is called on the right-hand-side even if RHS type=req.subjectType",
			expectedCode:            codes.OK,
			resourceObjectType:      "resource",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			dsInitFunc:              CustomDatastoreWithData,
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "resource", "resource1", "parent", "user", "user1_p", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("resource", "resource1", "user", "user1_p", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION),
				permissionUpdate("resource", "resource1", "user", "user3", "...", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requireInst := require.New(t)

			conn, cleanup, _, revision := testserver.NewTestServer(requireInst, 0, memdb.DisableGC, true, tc.dsInitFunc)
			t.Cleanup(cleanup)
			client := lookupwatchv1.NewLookupWatchServiceClient(conn)

			cursor := zedtoken.NewFromRevision(revision)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream, err := client.WatchAccessibleResources(
				ctx,
				&lookupwatchv1.WatchAccessibleResourcesRequest{
					ResourceObjectType:      tc.resourceObjectType,
					Permission:              tc.permission,
					SubjectObjectType:       tc.subjectObjectType,
					OptionalStartTimestamp:  cursor,
					OptionalSubjectRelation: tc.OptionalSubjectRelation,
				},
			)
			requireInst.NoError(err)

			updatesChan := make(chan []*lookupwatchv1.PermissionUpdate, len(tc.expectedUpdates))

			finishCode := false
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
								if errors.Is(err, io.EOF) {
									finishCode = true
								}
								break
							}

							panic(fmt.Errorf("received a stream read error: %w", err))
						}

						updatesChan <- resp.Updates
					}
				}
			}()

			_, err = v1.NewPermissionsServiceClient(conn).WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
				Updates: tc.mutations,
			})
			requireInst.NoError(err)

			var receivedUpdates []*lookupwatchv1.PermissionUpdate
			for len(receivedUpdates) < len(tc.expectedUpdates) || len(tc.expectedUpdates) == 0 {
				select {
				case updates := <-updatesChan:
					receivedUpdates = append(receivedUpdates, updates...)
				case <-time.After(1 * time.Second):
					if len(tc.expectedUpdates) == 0 {
						requireInst.Equal(true, finishCode)
						requireInst.Equal(0, len(receivedUpdates))
					} else {
						requireInst.FailNow("timed out waiting for updates")
					}
					return
				}
			}

			requireInst.Equal(sortPermissionUpdates(tc.expectedUpdates), sortPermissionUpdates(receivedUpdates))
		})
	}
}

func sortPermissionUpdates(in []*lookupwatchv1.PermissionUpdate) []*lookupwatchv1.PermissionUpdate {
	out := make([]*lookupwatchv1.PermissionUpdate, 0, len(in))
	out = append(out, in...)
	sort.Slice(out, func(i, j int) bool {
		left, right := out[i], out[j]
		compareResult := strings.Compare(left.Subject.Object.ObjectId, right.Subject.Object.ObjectId)
		if compareResult < 0 {
			return true
		}
		if compareResult > 0 {
			return false
		}

		return left.UpdatedPermission < right.UpdatedPermission
	})

	return out
}

func CustomDatastoreWithData(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	/**
	 * definition user {
	 *   relation viewer: user
	 *   permission view = viewer
	 * }
	 */
	var UserNS = ns.Namespace(
		"user",
		ns.Relation("viewer",
			nil,
			ns.AllowedRelation("user", "..."),
		),
		ns.Relation("view",
			ns.Union(
				ns.ComputedUserset("viewer"),
			),
		),
	)
	/*
	 * definition resource {
	 *   relation viewer: user
	 *   relation parent: resource | user
	 *   permission view = viewer + parent->view
	 * }
	 */
	var ResourceNS = ns.Namespace(
		"resource",
		ns.Relation("viewer",
			nil,
			ns.AllowedRelation("user", "..."),
		),
		ns.Relation("parent",
			nil,
			ns.AllowedRelation("resource", "..."),
			ns.AllowedRelation("user", "..."),
		),
		ns.Relation("view",
			ns.Union(
				ns.ComputedUserset("viewer"),
				ns.TupleToUserset("parent", "view"),
			),
		),
	)
	allDefs := []*core.NamespaceDefinition{UserNS, ResourceNS}
	ds, _ = DatastoreWithSchema(allDefs, ds, require)

	var tuples = []string{
		"resource:resource1#parent@resource:resource1_p#...",
		"resource:resource1_p#viewer@user:user1#...",
		"user:user1_p#viewer@user:user3",
		//"resource:resource1#parent@user:user1_p",
		//"user:user1#viewer@user:user1_p#...",
		//
		//"resource:resource2#parent@resource:resource2_p#...",
		"resource:resource2_p#viewer@user:user2#...",
	}
	ds, rev := DatastoreWithData(tuples, ds, require)
	return ds, rev
}

func DatastoreWithData(tupleValues []string, ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ctx := context.Background()

	tuples := make([]*core.RelationTuple, 0, len(tupleValues))
	for _, tupleStr := range tupleValues {
		tpl := tuple.Parse(tupleStr)
		require.NotNil(tpl)
		tuples = append(tuples, tpl)
	}
	revision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tuples...)
	require.NoError(err)

	return ds, revision
}

func DatastoreWithSchema(definitions []*core.NamespaceDefinition, ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ctx := context.Background()
	validating := testfixtures.NewValidatingDatastore(ds)

	newRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		for _, nsDef := range definitions {
			ts, err := namespace.BuildNamespaceTypeSystemWithFallback(nsDef, rwt, definitions)
			require.NoError(err)

			vts, err := ts.Validate(ctx)
			require.NoError(err)

			aerr := namespace.AnnotateNamespace(vts)
			require.NoError(aerr)

			err = rwt.WriteNamespaces(nsDef)
			require.NoError(err)
		}

		return nil
	})
	require.NoError(err)

	return validating, newRevision
}
