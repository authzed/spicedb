//go:build image && memoryprotection

package memoryprotection

import (
	"fmt"
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

	"github.com/authzed/spicedb/internal/middleware/memoryprotection/rtml"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

func init() {
	server.DefaultMemoryUsageProvider = rtml.NewRealTimeMemoryUsageProvider()
}

func TestServeWithMemoryProtectionMiddleware(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	container, err := sdbtestcontainer.Run(ctx, sdbtestcontainer.DefaultImageReference,
		testcontainers.WithEnv(map[string]string{
			"GOMEMLIMIT": "1B", // NOTE: Absurdly low on purpose
		}),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	mappedPort, err := container.MappedPort(ctx, "50051")
	require.NoError(t, err)
	serverPort := mappedPort.Port()

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", serverPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcutil.WithInsecureBearerToken(container.PresharedKey()),
	)

	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	// Health requests bypass the memory middleware
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	}, 5*time.Second, 1*time.Second, "server never became healthy")

	// Other requests have the memory middleware
	client := v1.NewSchemaServiceClient(conn)
	_, err = client.WriteSchema(t.Context(), &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.ResourceExhausted, s.Code())
}
