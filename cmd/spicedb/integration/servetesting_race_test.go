//go:build image

package integration_test

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

//go:embed testdata/bootstrap.yaml
var bootstrapContents string

// Based on a test originally written by https://github.com/wscalf
func TestCheckPermissionOnTesterNoFlakes(t *testing.T) {
	bootstrapReader := strings.NewReader(bootstrapContents)
	containerFilePath := "/mnt/spicedb_bootstrap.yaml"
	container, err := sdbtestcontainer.Run(t.Context(), sdbtestcontainer.DefaultImageReference,
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            bootstrapReader,
			ContainerFilePath: containerFilePath,
		}),
		testcontainers.WithEnv(map[string]string{
			"SPICEDB_LOAD_CONFIGS": containerFilePath,
		}),
	)
	require.NoError(t, err)

	for i := range 1000 {
		conn, err := grpc.NewClient(container.GRPCEndpoint(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)

		client := v1.NewPermissionsServiceClient(conn)
		result, err := client.CheckPermission(t.Context(), &v1.CheckPermissionRequest{
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
		conn.Close()

		require.NoError(t, err)
		require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result.Permissionship, "Error on attempt #%d", i)
	}
}
