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

			// The caches whose budgets are a percentage of available memory have
			// to be off for the server to start at all here. AvailableMemory() is
			// 75% of GOMEMLIMIT, which truncates to zero at this limit, and a
			// resolved budget of zero is not caught by CompleteCache's guard - that
			// tests the configured string for "0%" - so the cache is built with a
			// maximum weight of zero and otter rejects it: "weigher requires
			// maximumWeight". The server exits before serving and this test sees a
			// container that never becomes ready.
			//
			// Caches are irrelevant to what is being tested. The point is that the
			// memory middleware rejects requests when the limit is exhausted, which
			// it does regardless of what is cached.
			"SPICEDB_DISPATCH_CACHE_ENABLED":         "false",
			"SPICEDB_DISPATCH_CLUSTER_CACHE_ENABLED": "false",
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
