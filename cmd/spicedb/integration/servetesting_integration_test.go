//go:build image

package integration_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	readOnlyHTTPPort = "8444"
	readOnlyGRPCPort = "50052"
	defaultSchema    = `
definition user {}

definition resource {
	relation reader: user
	relation writer: user

	permission view = reader + writer
}
`
)

var defaultSchemaOption = sdbtestcontainer.WithBootstrapSchema(defaultSchema)

func TestTestServer(t *testing.T) {
	require := require.New(t)
	container, err := sdbtestcontainer.Run(t.Context(), ciImage,
		defaultSchemaOption,
		testcontainers.WithExposedPorts(readOnlyGRPCPort, readOnlyHTTPPort),
		testcontainers.WithCmd("serve-testing"),
	)
	require.NoError(err)
	testcontainers.CleanupContainer(t, container)

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(container.PresharedKey()),
	}
	conn, err := grpc.NewClient(container.GRPCEndpoint(), options...)
	require.NoError(err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	containerHost, err := container.Host(t.Context())
	readOnlyGRPCEndpoint := net.JoinHostPort(containerHost, readOnlyGRPCPort)
	require.NoError(err)
	roConn, err := grpc.NewClient(readOnlyGRPCEndpoint, options...)
	require.NoError(err)
	t.Cleanup(func() {
		_ = roConn.Close()
	})

	require.EventuallyWithT(func(collect *assert.CollectT) {
		resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if !assert.NoError(collect, err) {
			return
		}
		if !assert.Equal(collect, healthpb.HealthCheckResponse_SERVING, resp.GetStatus()) {
			return
		}

		resp, err = healthpb.NewHealthClient(roConn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	}, 5*time.Second, 5*time.Millisecond, "was unable to connect to running service(s)")

	v1client := v1.NewPermissionsServiceClient(conn)
	rov1client := v1.NewPermissionsServiceClient(roConn)

	relationship := tuple.MustParse("resource:someresource#reader@user:somegal")

	// Try writing a simple relationship against readonly and ensure it fails.
	_, err = rov1client.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(relationship)),
		},
	})
	require.Equal("rpc error: code = Unavailable desc = service read-only", err.Error())

	// Write a simple relationship.
	_, err = v1client.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(relationship)),
		},
	})
	require.NoError(err)

	// Ensure the check succeeds.
	checkReq := &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
		},
		Resource: &v1.ObjectReference{
			ObjectType: "resource",
			ObjectId:   "someresource",
		},
		Permission: "view",
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   "somegal",
			},
		},
	}

	v1Resp, err := v1client.CheckPermission(t.Context(), checkReq)
	require.NoError(err)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, v1Resp.Permissionship)

	// Ensure check against readonly works as well.
	v1Resp, err = rov1client.CheckPermission(t.Context(), checkReq)
	require.NoError(err)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, v1Resp.Permissionship)

	// Try a call with a different auth header and ensure it fails.
	authedConn, err := grpc.NewClient(readOnlyGRPCEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken("someothertoken"))
	require.NoError(err)
	t.Cleanup(func() {
		_ = authedConn.Close()
	})

	require.EventuallyWithT(func(collect *assert.CollectT) {
		resp, err := healthpb.NewHealthClient(authedConn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	}, 5*time.Second, 5*time.Millisecond, "was unable to connect to running service(s)")

	authedv1client := v1.NewPermissionsServiceClient(authedConn)
	_, err = authedv1client.CheckPermission(t.Context(), checkReq)
	s, ok := status.FromError(err)
	require.True(ok)
	require.Equal(codes.FailedPrecondition, s.Code())

	// Make an HTTP call and ensure it succeeds.
	httpEndpoint := container.HTTPEndpoint()
	require.NoError(err)
	readURL := fmt.Sprintf("http://%s/v1/schema/read", httpEndpoint)
	req, err := http.NewRequest("POST", readURL, nil)
	require.NoError(err)
	req.Header.Add("Authorization", "Bearer "+container.PresharedKey())
	hresp, err := http.DefaultClient.Do(req) //nolint:gosec  // SSRF isn't an issue in a test
	require.NoError(err)

	body, err := io.ReadAll(hresp.Body)
	require.NoError(err)

	t.Cleanup(func() {
		_ = hresp.Body.Close()
	})

	require.Equal(200, hresp.StatusCode)
	require.Contains(string(body), "schemaText")
	require.Contains(string(body), "definition resource")

	// Attempt to write to the read only HTTP and ensure it fails.
	readOnlyHTTPEndpoint := net.JoinHostPort(containerHost, readOnlyHTTPPort)
	writeURL := fmt.Sprintf("http://%s/v1/schema/write", readOnlyHTTPEndpoint)
	requestBody := strings.NewReader(`{
		"schemaText": "definition user {}\ndefinition resource {\nrelation reader: user\nrelation writer: user\nrelation foobar: user\n}"
	}`)
	//nolint:gosec  // this is test code
	wresp, err := http.Post(writeURL, "application/json", requestBody)
	t.Cleanup(func() {
		_ = wresp.Body.Close()
	})
	require.NoError(err)
	require.Equal(503, wresp.StatusCode)

	body, err = io.ReadAll(wresp.Body)
	require.NoError(err)
	require.Contains(string(body), "SERVICE_READ_ONLY")
}
