//go:build image

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
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
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

func TestServe(t *testing.T) {
	requireParent := require.New(t)

	// TODO:
	container, err := sdbtestcontainer.Run(t.Context(), sdbtestcontainer.DefaultImageReference,
	sdbtestcontainer.WithBootstrapSchema(defaultSchema),
	testcontainers.WithEnv(map[string]string{
		"SPICEDB_GRPC_PRESHARED_KEY": "firstkey,secondkey",
	}),
)
	requireParent.NoError(err)
	testcontainers.CleanupContainer(t, container)

	for key, expectedWorks := range map[string]bool{
		"":           false,
		"firstkey":   true,
		"secondkey":  true,
		"anotherkey": false,
	} {
		key := key
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

func gracefulShutdown(ctx context.Context, container testcontainers.Container) bool {
	closed := make(chan bool, 1)
	go func() {
		// Send SIGSTOP to have the container gracefully shutdown.
		_ = container.Stop(ctx, nil)
		closed <- true
	}()

	select {
	case <-closed:
		return true

	case <-time.After(10 * time.Second):
		_ = container.Terminate(ctx)
		return false
	}
}

// TODO: is this testing something useful? can we rewrite this?
func TestGracefulShutdownInMemory(t *testing.T) {
	ctx := t.Context()

	// Run a serve and immediately close, ensuring it shuts down gracefully.
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "authzed/spicedb:ci",
			Cmd:   []string{"serve", "--grpc-preshared-key", "firstkey"},
		},
		Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	require.True(t, gracefulShutdown(ctx, container))
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

			// TODO: supply a network?
			engine := testdatastore.RunDatastoreEngine(t, driverName)

			// TODO: figure out what's going on here
			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(env, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			db := engine.NewDatabase(t)

			// Run the migrate command and wait for it to complete.
			// TODO: figure out what the CI tag is
			migrateContainer, err := sdbtestcontainer.Run(ctx, sdbtestcontainer.DefaultImageReference,
			testcontainers.WithCmd("migrate", "head"),
			testcontainers.WithEnv(map[string]string{
				"SPICEDB_DATASTORE_ENGINE": driverName,
				"SPICEDB_DATASTORE_CONN_URI": db,
			}),
			testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
			)
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, migrateContainer)

			// Ensure the command completed successfully.
			exitCode, err := migrateContainer.State(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, exitCode.ExitCode)

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			ww := &logWaiter{c: make(chan bool, 1), expectedString: "running garbage collection worker"}
			serveReq := testcontainers.ContainerRequest{
				Image:              "authzed/spicedb:ci",
				Cmd:                []string{"serve", "--grpc-preshared-key", "firstkey", "--datastore-engine", driverName, "--datastore-conn-uri", db, "--datastore-gc-interval", "1s", "--telemetry-endpoint", ""},
				Env:                envVars,
			}
			if awaitGC {
				// Consume logs so we can ensure GC has run before starting a graceful shutdown.
				serveReq.LogConsumerCfg = &testcontainers.LogConsumerConfig{
					Consumers: []testcontainers.LogConsumer{ww},
				}
			}

			serveContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: serveReq,
				Started:          true,
			})
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, serveContainer)

			if awaitGC {
				select {
				case <-ww.c:
				case <-time.After(10 * time.Second):
					require.Fail(t, "timed out waiting for GC to run")
				}
			}

			require.True(t, gracefulShutdown(ctx, serveContainer))
		})
	}
}
