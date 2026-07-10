//go:build image

package integration_test

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

func TestServe(t *testing.T) {
	requireParent := require.New(t)

	container, err := sdbtestcontainer.Run(t.Context(), sdbtestcontainer.DefaultImageReference,
		sdbtestcontainer.WithBootstrapSchema(defaultSchema),
		sdbtestcontainer.WithPresharedKeys("firstkey", "secondkey"),
	)
	requireParent.NoError(err)
	testcontainers.CleanupContainer(t, container)

	for key, expectedWorks := range map[string]bool{
		"":           false,
		"firstkey":   true,
		"secondkey":  true,
		"anotherkey": false,
	} {
		t.Run(key, func(t *testing.T) {
			require := require.New(t)

			opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			if key != "" {
				opts = append(opts, grpcutil.WithInsecureBearerToken(key))
			}
			conn, err := grpc.NewClient(container.GRPCEndpoint(), opts...)

			require.NoError(err)
			t.Cleanup(func() {
				_ = conn.Close()
			})

			require.EventuallyWithT(func(collect *assert.CollectT) {
				resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
				if !assert.NoError(collect, err) {
					return
				}
				assert.Equal(collect, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
			}, 5*time.Second, 1*time.Millisecond, "was unable to connect to running service")

			client := v1.NewSchemaServiceClient(conn)
			_, err = client.WriteSchema(t.Context(), &v1.WriteSchemaRequest{
				Schema: `definition user {}`,
			})

			if expectedWorks {
				require.NoError(err)
			} else {
				s, ok := status.FromError(err)
				require.True(ok)

				if key == "" {
					require.Equal(codes.Unauthenticated, s.Code())
				} else {
					require.Equal(codes.PermissionDenied, s.Code())
				}
			}
		})
	}
}

// logWaiter is a testcontainers LogConsumer that signals on its channel the
// first time a log line containing expectedString is seen.
type logWaiter struct {
	c              chan bool
	expectedString string
}

var _ testcontainers.LogConsumer = (*logWaiter)(nil)

func (w *logWaiter) Accept(l testcontainers.Log) {
	if strings.Contains(string(l.Content), w.expectedString) {
		select {
		case w.c <- true:
		default:
		}
	}
}

func TestGracefulShutdown(t *testing.T) {
	engines := map[string]bool{
		"postgres":    true,
		"mysql":       true,
		"cockroachdb": false,
		"spanner":     false,
	}
	require.Len(t, datastore.Engines, len(engines))

	for driverName, awaitGC := range engines {
		t.Run(driverName, func(t *testing.T) {
			ctx := t.Context()

			// Create an internal network
			net, err := network.New(ctx)
			testcontainers.CleanupNetwork(t, net)
			require.NoError(t, err)

			engine := testdatastore.RunDatastoreEngine(t, driverName, network.WithNetwork([]string{driverName}, net))

			db := engine.NewDatabase(t)

			connectionVars, err := testutil.InternalConnectionEnvVars(db, driverName)
			require.NoError(t, err)

			// Run the migrate command and wait for it to complete.
			migrateContainer, err := testcontainers.Run(ctx, ciImage,
				network.WithNetwork([]string{"migrate"}, net),
				testcontainers.WithLogger(log.TestLogger(t)),
				testcontainers.WithCmd("migrate", "head"),
				testcontainers.WithEnv(connectionVars),
				testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
			)
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, migrateContainer)

			// Ensure the command completed successfully.
			containerState, err := migrateContainer.State(ctx)
			if containerState.ExitCode != 0 {
				logReader, err := migrateContainer.Logs(t.Context())
				require.NoError(t, err)
				out, err := io.ReadAll(logReader)
				require.NoError(t, err)
				t.Log("Container logs:")
				t.Log(string(out))
			}
			require.NoError(t, err)
			require.Equal(t, 0, containerState.ExitCode)

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			ww := &logWaiter{c: make(chan bool, 1), expectedString: "running garbage collection worker"}

			// Set the gc interval to 1s so we have something to look for in logs
			connectionVars["SPICEDB_DATASTORE_GC_INTERVAL"] = "1s"

			serveContainer, err := sdbtestcontainer.Run(ctx, ciImage,
				network.WithNetwork([]string{"spicedb"}, net),
				testcontainers.WithLogger(log.TestLogger(t)),
				testcontainers.WithEnv(connectionVars),
				testcontainers.WithLogConsumerConfig(&testcontainers.LogConsumerConfig{
					Consumers: []testcontainers.LogConsumer{ww},
				}),
			)
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, serveContainer)

			if awaitGC {
				select {
				case <-ww.c:
				case <-time.After(10 * time.Second):
					require.Fail(t, "timed out waiting for GC to run")
				}
			}
		})
	}
}
