//go:build docker && image && memoryprotection

package memoryprotection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/middleware/memoryprotection/rtml"
	"github.com/authzed/spicedb/pkg/cmd/server"
)

func init() {
	server.DefaultMemoryUsageProvider = rtml.NewRealTimeMemoryUsageProvider()
}

func TestServeWithMemoryProtectionMiddleware(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	serverToken := "mykey"
	serveResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "authzed/spicedb",
		Tag:          "ci",
		Cmd:          []string{"serve", "--log-level=debug", "--grpc-preshared-key", serverToken, "--telemetry-endpoint=\"\""},
		ExposedPorts: []string{"50051/tcp"},
		Env:          []string{"GOMEMLIMIT=1B"}, // NOTE: Absurdly low on purpose
	}, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(serveResource))
	})

	serverPort := serveResource.GetPort("50051/tcp")
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", serverPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(serverToken),
	)

	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	// Health requests bypass the memory middleware
	require.Eventually(t, func() bool {
		resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		return err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}, 5*time.Second, 1*time.Second, "server never became healthy")

	// Other requests have the memory middleware
	client := v1.NewSchemaServiceClient(conn)
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.ResourceExhausted, s.Code())
}
