package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Verifies the devcontext V1 service does not nil-panic on the
// metrics-recording paths (CheckPermission, CheckBulkPermissions,
// WriteRelationships) when no Metrics is provided in the config.
func TestDevContextV1ServiceMetricsPaths(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser").ToCoreTuple(),
		},
	})
	require.NoError(t, err)
	require.Nil(t, devErrs)

	conn, shutdown, err := devCtx.RunV1InMemoryService()
	require.NoError(t, err)
	t.Cleanup(shutdown)

	client := v1.NewPermissionsServiceClient(conn)

	checkResp, err := client.CheckPermission(t.Context(), &v1.CheckPermissionRequest{
		Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: "somedoc"},
		Permission: "view",
		Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "someuser"}},
	})
	require.NoError(t, err)
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, checkResp.Permissionship)

	bulkResp, err := client.CheckBulkPermissions(t.Context(), &v1.CheckBulkPermissionsRequest{
		Items: []*v1.CheckBulkPermissionsRequestItem{
			{
				Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: "somedoc"},
				Permission: "view",
				Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "someuser"}},
			},
			{
				Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: "somedoc"},
				Permission: "view",
				Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "nobody"}},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, bulkResp.Pairs, 2)

	_, err = client.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: tuple.ToV1Relationship(tuple.MustParse("document:somedoc#viewer@user:anotheruser")),
			},
		},
	})
	require.NoError(t, err)
}
