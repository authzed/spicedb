//go:build image

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
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
)

func TestServe(t *testing.T) {
	requireParent := require.New(t)

	// TODO:
	tester, err := newTester(t,
		testcontainers.ContainerRequest{
			Image:        "authzed/spicedb:ci",
			Cmd:          []string{"serve", "--log-level", "debug", "--grpc-preshared-key", "firstkey", "--grpc-preshared-key", "secondkey"},
			ExposedPorts: []string{"50051/tcp"},
		},
		"firstkey",
		false,
	)
	requireParent.NoError(err)

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
			conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", tester.port), opts...)

			require.NoError(err)
			t.Cleanup(func() {
				_ = conn.Close()
			})

			require.Eventually(func() bool {
				resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
				if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
					return false
				}

				return true
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
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	require.True(t, gracefulShutdown(ctx, container))
}

// hostInternalize rewrites localhost references so a SpiceDB container can
// reach a datastore listening on a host-mapped port via host.docker.internal
// (paired with withHostGateway).
func hostInternalize(uri string) string {
	return strings.ReplaceAll(uri, "localhost", "host.docker.internal")
}

// withHostGateway maps host.docker.internal to the host gateway. Docker Desktop
// resolves it automatically, but Linux (e.g. CI runners) does not without this.
func withHostGateway(hc *dockercontainer.HostConfig) {
	hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
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

			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(hostInternalize(env), "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// The datastore listens on a host-mapped port, so the SpiceDB
			// container must reach it via host.docker.internal.
			db := hostInternalize(engine.NewDatabase(t))

			// Run the migrate command and wait for it to complete.
			migrateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:              "authzed/spicedb:ci",
					Cmd:                []string{"migrate", "head", "--datastore-engine", driverName, "--datastore-conn-uri", db},
					Env:                envVars,
					HostConfigModifier: withHostGateway,
					WaitingFor:         wait.ForExit().WithExitTimeout(time.Minute),
				},
				Started: true,
			})
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
				HostConfigModifier: withHostGateway,
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
