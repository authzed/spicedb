//go:build docker && image
// +build docker,image

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestTestServer(t *testing.T) {
	require := require.New(t)
	key := uuid.NewString()
	tester, err := newTester(t,
		&dockertest.RunOptions{
			Repository: "authzed/spicedb",
			Tag:        "ci",
			Cmd: []string{
				"serve-testing",
				"--log-level", "debug",
				"--http-addr", ":8443",
				"--readonly-http-addr", ":8444",
				"--http-enabled",
				"--readonly-http-enabled",
			},
			ExposedPorts: []string{"50051/tcp", "50052/tcp", "8443/tcp", "8444/tcp"},
		},
		key,
		false,
	)
	require.NoError(err)
	defer tester.cleanup()

	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpcutil.WithInsecureBearerToken(key)}
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.port), options...)
	require.NoError(err)
	defer conn.Close()

	roConn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.readonlyPort), options...)
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
	authedConn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.readonlyPort), grpc.WithTransportCredentials(insecure.NewCredentials()), grpcutil.WithInsecureBearerToken("someothertoken"))
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
	readUrl := fmt.Sprintf("http://localhost:%s/v1/schema/read", tester.httpPort)
	req, err := http.NewRequest("POST", readUrl, nil)
	req.Header.Add("Authorization", "Bearer "+key)
	hresp, err := http.DefaultClient.Do(req)
	require.NoError(err)

	body, err := io.ReadAll(hresp.Body)
	require.NoError(err)

	require.Equal(200, hresp.StatusCode)
	require.Contains(string(body), "schemaText")
	require.Contains(string(body), "definition resource")

	// Attempt to write to the read only HTTP and ensure it fails.
	writeUrl := fmt.Sprintf("http://localhost:%s/v1/schema/write", tester.readonlyHttpPort)
	wresp, err := http.Post(writeUrl, "application/json", strings.NewReader(`{
		"schemaText": "definition user {}\ndefinition resource {\nrelation reader: user\nrelation writer: user\nrelation foobar: user\n}"
	}`))
	require.NoError(err)
	require.Equal(503, wresp.StatusCode)

	body, err = ioutil.ReadAll(wresp.Body)
	require.NoError(err)
	require.Contains(string(body), "SERVICE_READ_ONLY")
}

type spicedbHandle struct {
	port             string
	readonlyPort     string
	httpPort         string
	readonlyHttpPort string
	cleanup          func()
}

const retryCount = 8

func newTester(t *testing.T, containerOpts *dockertest.RunOptions, token string, withExistingSchema bool) (*spicedbHandle, error) {
	for i := 0; i < retryCount; i++ {
		pool, err := dockertest.NewPool("")
		if err != nil {
			return nil, fmt.Errorf("could not connect to docker: %w", err)
		}

		pool.MaxWait = 60 * time.Second

		resource, err := pool.RunWithOptions(containerOpts)
		if err != nil {
			return nil, fmt.Errorf("could not start resource: %w", err)
		}

		port := resource.GetPort("50051/tcp")
		readonlyPort := resource.GetPort("50052/tcp")
		httpPort := resource.GetPort("8443/tcp")
		readonlyHttpPort := resource.GetPort("8444/tcp")

		cleanup := func() {
			// When you're done, kill and remove the container
			if err = pool.Purge(resource); err != nil {
				log.Fatalf("Could not purge resource: %s", err)
			}
		}

		// Give the service time to boot.
		err = pool.Retry(func() error {
			conn, err := grpc.Dial(
				fmt.Sprintf("localhost:%s", port),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpcutil.WithInsecureBearerToken(token),
			)
			if err != nil {
				return err
			}

			client := v1.NewSchemaServiceClient(conn)

			if withExistingSchema {
				_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
				return err
			}

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

			return err
		})
		if err != nil {
			stream := new(bytes.Buffer)

			waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			lerr := pool.Client.Logs(docker.LogsOptions{
				Context:      waitCtx,
				OutputStream: stream,
				ErrorStream:  stream,
				Stdout:       true,
				Stderr:       true,
				Container:    resource.Container.ID,
			})
			require.NoError(t, lerr)

			fmt.Printf("got error on startup: %v\ncontainer logs: %s\n", err, stream.String())
			cleanup()
			continue
		}

		return &spicedbHandle{
			port:             port,
			readonlyPort:     readonlyPort,
			httpPort:         httpPort,
			readonlyHttpPort: readonlyHttpPort,
			cleanup:          cleanup,
		}, nil
	}

	return nil, fmt.Errorf("hit maximum retries when trying to spawn test server")
}
