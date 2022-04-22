//go:build docker
// +build docker

package main

import (
	"context"
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
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
	)
	requireParent.NoError(err)
	defer tester.cleanup()

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
			conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", tester.port), opts...)

			require.NoError(err)
			defer conn.Close()

			resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "authzed.api.v1.SchemaService"})
			require.NoError(err)
			require.Equal(healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

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
