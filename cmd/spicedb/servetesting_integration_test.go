//go:build docker
// +build docker

package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTestServer(t *testing.T) {
	tester, err := newTester(t,
		&dockertest.RunOptions{
			Repository:   "authzed/spicedb",
			Tag:          "latest",
			Cmd:          []string{"serve-testing", "--log-level", "debug"},
			ExposedPorts: []string{"50051/tcp", "50052/tcp"},
		},
	)
	require.NoError(t, err)
	defer tester.cleanup()

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.port), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	roConn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.readonlyPort), grpc.WithInsecure())
	require.NoError(t, err)
	defer roConn.Close()

	v0client := v0.NewACLServiceClient(conn)
	rov0client := v0.NewACLServiceClient(roConn)

	v1client := v1.NewPermissionsServiceClient(conn)
	rov1client := v1.NewPermissionsServiceClient(roConn)

	tpl := &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: "resource",
			ObjectId:  "someresource",
			Relation:  "reader",
		},
		User: &v0.User{UserOneof: &v0.User_Userset{Userset: &v0.ObjectAndRelation{
			Namespace: "user",
			ObjectId:  "somegal",
			Relation:  "...",
		}}},
	}

	// Try writing a simple relationship against readonly and ensure it fails.
	_, err = rov0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{
			{
				Operation: v0.RelationTupleUpdate_CREATE,
				Tuple:     tpl,
			},
		},
	})
	require.Equal(t, "rpc error: code = Unavailable desc = service read-only", err.Error())

	// Write a simple relationship.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{
			{
				Operation: v0.RelationTupleUpdate_CREATE,
				Tuple:     tpl,
			},
		},
	})
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, v1Resp.Permissionship)

	// Ensure check against readonly works as well.
	v1Resp, err = rov1client.CheckPermission(context.Background(), checkReq)
	require.NoError(t, err)
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, v1Resp.Permissionship)

	// Try a call with a different auth header and ensure it fails.
	authedConn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.readonlyPort), grpc.WithInsecure(), grpcutil.WithInsecureBearerToken("someothertoken"))
	require.NoError(t, err)
	defer authedConn.Close()

	authedv1client := v1.NewPermissionsServiceClient(authedConn)
	v1Resp, err = authedv1client.CheckPermission(context.Background(), checkReq)
	require.Equal(t, "rpc error: code = FailedPrecondition desc = failed precondition: object definition `resource` not found", err.Error())
}

type spicedbHandle struct {
	port         string
	readonlyPort string
	cleanup      func()
}

func newTester(t *testing.T, containerOpts *dockertest.RunOptions) (*spicedbHandle, error) {
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

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	// Give the service time to boot.
	require.Eventually(t, func() bool {
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", port), grpc.WithInsecure())
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
		return err == nil
	}, 1*time.Second, 10*time.Millisecond, "could not start test server")

	return &spicedbHandle{port: port, readonlyPort: readonlyPort, cleanup: cleanup}, nil
}
