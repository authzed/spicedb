package sdbtestcontainer_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

const testSchema = `
definition user {}

definition resource {
	relation reader: user
	relation writer: user

	permission view = reader + writer
}
`

func TestRun(t *testing.T) {
	ctx := t.Context()

	ctr, err := sdbtestcontainer.Run(ctx, sdbtestcontainer.DefaultImageReference)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, ctr)

	require.NotEmpty(t, ctr.GRPCEndpoint())
	require.NotEmpty(t, ctr.PresharedKey())

	// HTTP is not enabled by default.
	httpEndpoint := ctr.HTTPEndpoint()
	require.Empty(t, httpEndpoint)

	conn := dial(t, ctr)
	_, err = v1.NewSchemaServiceClient(conn).WriteSchema(ctx, &v1.WriteSchemaRequest{Schema: testSchema})
	require.NoError(t, err)
}

func TestRunRejectsWrongKey(t *testing.T) {
	ctx := t.Context()

	ctr, err := sdbtestcontainer.Run(ctx, sdbtestcontainer.DefaultImageReference)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, ctr)

	conn, err := grpc.NewClient(
		ctr.GRPCEndpoint(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken("not-the-key"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = v1.NewSchemaServiceClient(conn).ReadSchema(ctx, &v1.ReadSchemaRequest{})
	require.Error(t, err)
}

func TestRunWithHTTP(t *testing.T) {
	ctx := t.Context()

	ctr, err := sdbtestcontainer.Run(ctx,
		sdbtestcontainer.DefaultImageReference,
		sdbtestcontainer.WithHTTP(),
		sdbtestcontainer.WithBootstrapSchema(testSchema),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, ctr)

	httpEndpoint := ctr.HTTPEndpoint()
	require.NoError(t, err)
	require.NotEmpty(t, httpEndpoint)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+httpEndpoint+"/v1/schema/read", nil)
	require.NoError(t, err)
	req.Header.Add("Authorization", "Bearer "+ctr.PresharedKey())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), "definition resource")
}

func TestRunWithBootstrap(t *testing.T) {
	ctx := t.Context()

	ctr, err := sdbtestcontainer.Run(ctx,
		sdbtestcontainer.DefaultImageReference,
		sdbtestcontainer.WithBootstrapSchema(testSchema),
		sdbtestcontainer.WithBootstrapRelationships("resource:someresource#reader@user:somegal"),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, ctr)

	conn := dial(t, ctr)
	resp, err := v1.NewPermissionsServiceClient(conn).CheckPermission(ctx, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
		Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: "someresource"},
		Permission:  "view",
		Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "somegal"}},
	})
	require.NoError(t, err)
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, resp.GetPermissionship())
}

func dial(t *testing.T, ctr *sdbtestcontainer.Container) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		ctr.GRPCEndpoint(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(ctr.PresharedKey()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
