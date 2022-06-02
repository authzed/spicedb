//go:build docker
// +build docker

package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/ory/dockertest/v3"
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

	tester, err := newTester(t,
		&dockertest.RunOptions{
			Repository:   "authzed/spicedb",
			Tag:          "ci",
			Cmd:          []string{"serve-testing", "--log-level", "debug"},
			ExposedPorts: []string{"50051/tcp", "50052/tcp"},
		},
		"",
	)
	require.NoError(err)
	defer tester.cleanup()

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(err)
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1alpha1.SchemaService"})
	require.NoError(err)
	require.Equal(healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	roConn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.readonlyPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(err)
	defer roConn.Close()

	resp, err = healthpb.NewHealthClient(roConn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1alpha1.SchemaService"})
	require.NoError(err)
	require.Equal(healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	v1client := v1.NewPermissionsServiceClient(conn)
	rov1client := v1.NewPermissionsServiceClient(roConn)

	relationship := tuple.MustParse("resource:someresource#reader@user:somegal")

	// Try writing a simple relationship against readonly and ensure it fails.
	_, err = rov1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.UpdateToRelationshipUpdate(tuple.Create(relationship)),
		},
	})
	require.Equal("rpc error: code = Unavailable desc = service read-only", err.Error())

	// Write a simple relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.UpdateToRelationshipUpdate(tuple.Create(relationship)),
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
	authedConn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.readonlyPort), grpc.WithInsecure(), grpcutil.WithInsecureBearerToken("someothertoken"))
	require.NoError(err)
	defer authedConn.Close()

	authedv1client := v1.NewPermissionsServiceClient(authedConn)
	_, err = authedv1client.CheckPermission(context.Background(), checkReq)
	s, ok := status.FromError(err)
	require.True(ok)
	require.Equal(codes.FailedPrecondition, s.Code())
}

type spicedbHandle struct {
	port         string
	readonlyPort string
	httpPort     string
	cleanup      func()
}

func newTester(t *testing.T, containerOpts *dockertest.RunOptions, token string) (*spicedbHandle, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("Could not connect to docker: %w", err)
	}

	resource, err := pool.RunWithOptions(containerOpts)
	if err != nil {
		return nil, fmt.Errorf("Could not start resource: %w", err)
	}

	port := resource.GetPort("50051/tcp")
	readonlyPort := resource.GetPort("50052/tcp")
	httpPort := resource.GetPort("8443/tcp")

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	// Give the service time to boot.
	require.Eventually(t, func() bool {
		conn, err := grpc.Dial(
			fmt.Sprintf("localhost:%s", port),
			grpc.WithInsecure(),
			grpcutil.WithInsecureBearerToken(token),
		)
		if err != nil {
			return false
		}

		client := v1alpha1.NewSchemaServiceClient(conn)

		// Write a basic schema.
		_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
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
			s, ok := status.FromError(err)
			require.True(t, !ok || s.Code() == codes.Unavailable, fmt.Sprintf("Found unexpected error: %v", err))
		}
		return err == nil
	}, 3*time.Second, 10*time.Millisecond, "could not start test server")

	return &spicedbHandle{port: port, readonlyPort: readonlyPort, httpPort: httpPort, cleanup: cleanup}, nil
}
