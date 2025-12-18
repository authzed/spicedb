//go:build docker && image

package integration_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
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
				resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
				if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
					return false
				}

				return true
			}, 5*time.Second, 1*time.Millisecond, "was unable to connect to running service")

			client := v1.NewSchemaServiceClient(conn)
			_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
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

func TestGracefulShutdownInMemory(t *testing.T) {
	ctx := context.Background()

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

type watchingWriter struct {
	c              chan bool
	expectedString string
}

func (ww *watchingWriter) Write(p []byte) (n int, err error) {
	if strings.Contains(string(p), ww.expectedString) {
		ww.c <- true
	}

	return len(p), nil
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
			ctx := context.Background()
			bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())

			// Create a bridge network for testing.
			net, err := network.New(ctx, network.WithDriver("bridge"), network.WithLabels(map[string]string{"name": bridgeNetworkName}))
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = net.Remove(ctx)
			})

			engine := testdatastore.RunDatastoreEngineWithBridge(t, driverName, bridgeNetworkName)

			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(env, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// Run the migrate command and wait for it to complete.
			db := engine.NewDatabase(t)
			migrateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:    "authzed/spicedb:ci",
					Cmd:      []string{"migrate", "head", "--datastore-engine", driverName, "--datastore-conn-uri", db},
					Networks: []string{bridgeNetworkName},
					Env:      envVars,
				},
				Started: true,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = migrateContainer.Terminate(ctx)
			})

			// Ensure the command completed successfully.
			exitCode, err := migrateContainer.State(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, exitCode.ExitCode)

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			serveContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:    "authzed/spicedb:ci",
					Cmd:      []string{"serve", "--grpc-preshared-key", "firstkey", "--datastore-engine", driverName, "--datastore-conn-uri", db, "--datastore-gc-interval", "1s", "--telemetry-endpoint", ""},
					Networks: []string{bridgeNetworkName},
					Env:      envVars,
				},
				Started: true,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = serveContainer.Terminate(ctx)
			})

			if awaitGC {
				ww := &watchingWriter{make(chan bool, 1), "running garbage collection worker"}

				// Grab logs and ensure GC has run before starting a graceful shutdown.
				go (func() {
					logReader, err := serveContainer.Logs(ctx)
					if err != nil {
						assert.NoError(t, err)
						return
					}
					defer logReader.Close()
					_, err = io.Copy(ww, logReader)
					assert.NoError(t, err)
				})()

				select {
				case <-ww.c:
					break

				case <-time.After(10 * time.Second):
					require.Fail(t, "timed out waiting for GC to run")
				}
			}

			require.True(t, gracefulShutdown(ctx, serveContainer))
		})
	}
}
