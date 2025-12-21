//go:build docker && image

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestTestServer(t *testing.T) {
	require := require.New(t)
	key := uuid.NewString()
	tester, err := newTester(t,
		testcontainers.ContainerRequest{
			Image: "authzed/spicedb:ci",
			Cmd: []string{
				"serve-testing",
				"--log-level", "debug",
				"--http-addr", ":8443",
				"--readonly-http-addr", ":8444",
				"--http-enabled",
				// "--readonly-http-enabled",
			},
			ExposedPorts: []string{"50051/tcp", "50052/tcp", "8443/tcp", "8444/tcp"},
		},
		key,
		false,
	)
	require.NoError(err)

	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpcutil.WithInsecureBearerToken(key)}
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", tester.port), options...)
	require.NoError(err)
	defer conn.Close()

	roConn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", tester.readonlyPort), options...)
	require.NoError(err)
	defer roConn.Close()

	require.Eventually(func() bool {
		resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return false
		}

		resp, err = healthpb.NewHealthClient(roConn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return false
		}

		return true
	}, 5*time.Second, 5*time.Millisecond, "was unable to connect to running service(s)")

	v1client := v1.NewPermissionsServiceClient(conn)
	rov1client := v1.NewPermissionsServiceClient(roConn)

	relationship := tuple.MustParse("resource:someresource#reader@user:somegal")

	// Try writing a simple relationship against readonly and ensure it fails.
	_, err = rov1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(relationship)),
		},
	})
	require.Equal("rpc error: code = Unavailable desc = service read-only", err.Error())

	// Write a simple relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
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

	v1Resp, err := v1client.CheckPermission(context.Background(), checkReq)
	require.NoError(err)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, v1Resp.Permissionship)

	// Ensure check against readonly works as well.
	v1Resp, err = rov1client.CheckPermission(context.Background(), checkReq)
	require.NoError(err)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, v1Resp.Permissionship)

	// Try a call with a different auth header and ensure it fails.
	authedConn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", tester.readonlyPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken("someothertoken"))
	require.NoError(err)
	defer authedConn.Close()

	require.Eventually(func() bool {
		resp, err := healthpb.NewHealthClient(authedConn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return false
		}

		return true
	}, 5*time.Second, 5*time.Millisecond, "was unable to connect to running service(s)")

	authedv1client := v1.NewPermissionsServiceClient(authedConn)
	_, err = authedv1client.CheckPermission(context.Background(), checkReq)
	s, ok := status.FromError(err)
	require.True(ok)
	require.Equal(codes.FailedPrecondition, s.Code())

	// Make an HTTP call and ensure it succeeds.
	readURL := fmt.Sprintf("http://localhost:%s/v1/schema/read", tester.HTTPPort)
	req, err := http.NewRequest("POST", readURL, nil)
	require.NoError(err)
	req.Header.Add("Authorization", "Bearer "+key)
	hresp, err := http.DefaultClient.Do(req)
	require.NoError(err)

	body, err := io.ReadAll(hresp.Body)
	require.NoError(err)

	t.Cleanup(func() {
		_ = hresp.Body.Close()
	})

	require.Equal(200, hresp.StatusCode)
	require.Contains(string(body), "schemaText")
	require.Contains(string(body), "definition resource")

	/*
		 * TODO(jschorr): Re-enable once we figure out why this makes the test flaky
		// Attempt to write to the read only HTTP and ensure it fails.
		writeUrl := fmt.Sprintf("http://localhost:%s/v1/schema/write", tester.readonlyHTTPPort)
		wresp, err := http.Post(writeUrl, "application/json", strings.NewReader(`{
			"schemaText": "definition user {}\ndefinition resource {\nrelation reader: user\nrelation writer: user\nrelation foobar: user\n}"
		}`))
		require.NoError(err)
		require.Equal(503, wresp.StatusCode)

		body, err = ioutil.ReadAll(wresp.Body)
		require.NoError(err)
		require.Contains(string(body), "SERVICE_READ_ONLY")
	*/
}

type spicedbHandle struct {
	port             string
	readonlyPort     string
	HTTPPort         string
	readonlyHTTPPort string
}

const retryCount = 8

// newTester spins up a SpiceDB server running against a specific datastore with a specific access token.
// It also writes or reads a schema.
// On test termination it cleans up all resources.
func newTester(t *testing.T, containerReq testcontainers.ContainerRequest, token string, withExistingSchema bool) (*spicedbHandle, error) {
	ctx := context.Background()

	for i := 0; i < retryCount; i++ {
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: containerReq,
			Started:          true,
		})
		if err != nil {
			return nil, fmt.Errorf("could not start resource: %w", err)
		}

		t.Cleanup(func() {
			_ = container.Terminate(ctx)
		})

		mappedPort, err := container.MappedPort(ctx, "50051")
		if err != nil {
			return nil, fmt.Errorf("could not get port: %w", err)
		}
		port := mappedPort.Port()

		mappedReadonlyPort, err := container.MappedPort(ctx, "50052")
		if err != nil {
			return nil, fmt.Errorf("could not get readonly port: %w", err)
		}
		readonlyPort := mappedReadonlyPort.Port()

		mappedHTTPPort, err := container.MappedPort(ctx, "8443")
		if err != nil {
			return nil, fmt.Errorf("could not get HTTP port: %w", err)
		}
		httpPort := mappedHTTPPort.Port()

		mappedReadonlyHTTPPort, err := container.MappedPort(ctx, "8444")
		if err != nil {
			return nil, fmt.Errorf("could not get readonly HTTP port: %w", err)
		}
		readonlyHTTPPort := mappedReadonlyHTTPPort.Port()

		// Give the service time to boot.
		maxRetries := 30
		var lastErr error
		for j := 0; j < maxRetries; j++ {
			conn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%s", port),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpcutil.WithInsecureBearerToken(token),
			)
			if err != nil {
				lastErr = fmt.Errorf("could not create connection: %w", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			t.Cleanup(func() {
				_ = conn.Close()
			})

			client := v1.NewSchemaServiceClient(conn)

			if withExistingSchema {
				_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
				if err != nil {
					lastErr = err
					time.Sleep(500 * time.Millisecond)
					continue
				}
			} else {
				// Write a basic schema.
				_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
					Schema: `
			definition user {}

			definition resource {
				relation reader: user
				relation writer: user

				permission view = reader + writer
			}
			`,
				})
				if err != nil {
					lastErr = err
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}

			return &spicedbHandle{
				port:             port,
				readonlyPort:     readonlyPort,
				HTTPPort:         httpPort,
				readonlyHTTPPort: readonlyHTTPPort,
			}, nil
		}

		// If we got here, retries failed
		logs, err := container.Logs(ctx)
		if err == nil {
			stream := new(bytes.Buffer)
			_, _ = io.Copy(stream, logs)
			fmt.Printf("got error on startup: %v\ncontainer logs: %s\n", lastErr, stream.String())
		} else {
			fmt.Printf("got error on startup: %v (could not retrieve logs: %v)\n", lastErr, err)
		}
		continue
	}

	return nil, fmt.Errorf("hit maximum retries when trying to boot SpiceDB server")
}
