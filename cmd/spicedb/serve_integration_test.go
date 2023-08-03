//go:build docker && image
// +build docker,image

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestServe(t *testing.T) {
	requireParent := require.New(t)

	tester, err := newTester(t,
		&dockertest.RunOptions{
			Repository:   "authzed/spicedb",
			Tag:          "ci",
			Cmd:          []string{"serve", "--log-level", "debug", "--grpc-preshared-key", "firstkey", "--grpc-preshared-key", "secondkey"},
			ExposedPorts: []string{"50051/tcp"},
		},
		"firstkey",
		false,
	)
	requireParent.NoError(err)
	defer tester.cleanup()

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
			conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.port), opts...)

			require.NoError(err)
			defer conn.Close()

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

func gracefulShutdown(pool *dockertest.Pool, serveResource *dockertest.Resource) bool {
	closed := make(chan bool, 1)
	go func() {
		// Send SIGSTOP to have the container gracefully shutdown.
		pool.Client.KillContainer(docker.KillContainerOptions{
			ID:      serveResource.Container.ID,
			Signal:  docker.SIGSTOP,
			Context: context.Background(),
		})
		closed <- true
	}()

	select {
	case <-closed:
		return true

	case <-time.After(10 * time.Second):
		_ = pool.Purge(serveResource)
		return false
	}
}

func TestGracefulShutdownInMemory(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Run a serve and immediately close, ensuring it shuts down gracefully.
	serveResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "authzed/spicedb",
		Tag:        "ci",
		Cmd:        []string{"serve", "--grpc-preshared-key", "firstkey"},
	}, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	require.NoError(t, err)

	require.True(t, gracefulShutdown(pool, serveResource))
}

type watchingWriter struct {
	c chan bool
}

func (ww *watchingWriter) Write(p []byte) (n int, err error) {
	// Ensure GC ran.
	if strings.Contains(string(p), "running garbage collection worker") {
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
	require.Equal(t, len(engines), len(datastore.Engines))

	for driverName, awaitGC := range engines {
		t.Run(driverName, func(t *testing.T) {
			bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			// Create a bridge network for testing.
			network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{
				Name: bridgeNetworkName,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				pool.Client.RemoveNetwork(network.ID)
			})

			engine := testdatastore.RunDatastoreEngineWithBridge(t, driverName, bridgeNetworkName)

			envVars := []string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				envVars = wev.ExternalEnvVars()
			}

			// Run the migrate command and wait for it to complete.
			db := engine.NewDatabase(t)
			migrateResource, err := pool.RunWithOptions(&dockertest.RunOptions{
				Repository: "authzed/spicedb",
				Tag:        "ci",
				Cmd:        []string{"migrate", "head", "--datastore-engine", driverName, "--datastore-conn-uri", db},
				NetworkID:  bridgeNetworkName,
				Env:        envVars,
			}, func(config *docker.HostConfig) {
				config.RestartPolicy = docker.RestartPolicy{
					Name: "no",
				}
			})
			require.NoError(t, err)

			waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Ensure the command completed successfully.
			status, err := pool.Client.WaitContainerWithContext(migrateResource.Container.ID, waitCtx)
			require.NoError(t, err)
			require.Equal(t, 0, status)

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			serveResource, err := pool.RunWithOptions(&dockertest.RunOptions{
				Repository: "authzed/spicedb",
				Tag:        "ci",
				Cmd:        []string{"serve", "--grpc-preshared-key", "firstkey", "--datastore-engine", driverName, "--datastore-conn-uri", db, "--datastore-gc-interval", "1s", "--telemetry-endpoint", ""},
				NetworkID:  bridgeNetworkName,
				Env:        envVars,
			}, func(config *docker.HostConfig) {
				config.RestartPolicy = docker.RestartPolicy{
					Name: "no",
				}
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = pool.Purge(serveResource)
			})

			if awaitGC {
				ww := &watchingWriter{make(chan bool, 1)}

				// Grab logs and ensure GC has run before starting a graceful shutdown.
				opts := docker.LogsOptions{
					Context:      context.Background(),
					Stderr:       true,
					Stdout:       true,
					Follow:       true,
					Timestamps:   true,
					RawTerminal:  true,
					Container:    serveResource.Container.ID,
					OutputStream: ww,
				}

				go (func() {
					err = pool.Client.Logs(opts)
					require.NoError(t, err)
				})()

				select {
				case <-ww.c:
					break

				case <-time.After(10 * time.Second):
					require.Fail(t, "timed out waiting for GC to run")
				}
			}

			require.True(t, gracefulShutdown(pool, serveResource))
		})
	}
}
