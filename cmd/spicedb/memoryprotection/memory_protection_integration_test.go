//go:build docker && image && memoryprotection

package memoryprotection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
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

	ctx := context.Background()
	serverToken := "mykey"

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "authzed/spicedb:ci",
			Cmd:          []string{"serve", "--log-level=debug", "--grpc-preshared-key", serverToken, "--telemetry-endpoint=\"\""},
			ExposedPorts: []string{"50051/tcp"},
			Env: map[string]string{
				"GOMEMLIMIT": "1B", // NOTE: Absurdly low on purpose
			},
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	mappedPort, err := container.MappedPort(ctx, "50051")
	require.NoError(t, err)
	serverPort := mappedPort.Port()

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
