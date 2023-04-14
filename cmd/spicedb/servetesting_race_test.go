//go:build docker && image
// +build docker,image

package main

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Based on a test originally written by https://github.com/wscalf
func TestCheckPermissionOnTesterNoFlakes(t *testing.T) {
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)
	tester, err := newTester(t,
		&dockertest.RunOptions{
			Repository:   "authzed/spicedb",
			Tag:          "ci",
			Cmd:          []string{"serve-testing", "--load-configs", "/mnt/spicedb_bootstrap.yaml"},
			Mounts:       []string{path.Join(basepath, "testdata/bootstrap.yaml") + ":/mnt/spicedb_bootstrap.yaml"},
			ExposedPorts: []string{"50051/tcp", "50052/tcp", "8443/tcp", "8444/tcp"},
		},
		"",
		true,
	)
	require.NoError(t, err)
	defer tester.cleanup()

	for i := 0; i < 1000; i++ {
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		client := v1.NewPermissionsServiceClient(conn)

		result, err := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
			Resource: &v1.ObjectReference{
				ObjectType: "access",
				ObjectId:   "blue",
			},
			Permission: "assigned",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   "alice",
				},
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result.Permissionship, "Error on attempt #%d", i)
	}
}
