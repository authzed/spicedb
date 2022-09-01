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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	lookupwatchv1 "github.com/authzed/spicedb/pkg/proto/lookupwatch/v1"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func permissionUpdate(
	resourceType string,
	resourceId string,
	subjectType string,
	subjectId string,
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
		},
		UpdatedPermission: permission,
	}
}

func TestLookupWatch(t *testing.T) {
	testCases := []struct {
		name                    string
		resourceObjectType      string
		permission              string
		subjectObjectType       string
		OptionalSubjectRelation string
		startCursor             *v1.ZedToken
		mutations               []*v1.RelationshipUpdate
		expectedCode            codes.Code
		expectedUpdates         []*lookupwatchv1.PermissionUpdate
	}{
		{
			name:                    "lookupWatch basic test relation CREATE",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "document1", "viewer", "user", "user1", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("document", "document1", "user", "user1", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
			},
		},
		{
			name:                    "lookupWatch basic test relation change on an intermediate relation",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_DELETE, "folder", "company", "viewer", "folder", "auditors", "viewer"),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("document", "companyplan", "user", "auditor", v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION),
				permissionUpdate("document", "masterplan", "user", "auditor", v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION),
			},
		},
		{
			name:                    "lookupWatch basic test with no change impact on the watched resourceType",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "folder", "company", "parent", "folder", "aNewFolder", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{},
		},
		{
			name:                    "lookupWatch adding an intermediate relation",
			expectedCode:            codes.OK,
			resourceObjectType:      "document",
			permission:              "view",
			subjectObjectType:       "user",
			OptionalSubjectRelation: "...",
			mutations: []*v1.RelationshipUpdate{
				update(v1.RelationshipUpdate_OPERATION_CREATE, "folder", "new_folder", "parent", "folder", "company", ""),
				update(v1.RelationshipUpdate_OPERATION_CREATE, "document", "new_document", "parent", "folder", "new_folder", ""),
			},
			expectedUpdates: []*lookupwatchv1.PermissionUpdate{
				permissionUpdate("document", "new_document", "user", "legal", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
				permissionUpdate("document", "new_document", "user", "owner", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
				permissionUpdate("document", "new_document", "user", "auditor", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requireInst := require.New(t)

			conn, cleanup, _, revision := testserver.NewTestServer(requireInst, 0, memdb.DisableGC, true, testfixtures.StandardDatastoreWithData)
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
