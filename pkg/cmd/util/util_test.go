package util

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestDisabledGRPC(t *testing.T) {
	s, err := (&GRPCServerConfig{Enabled: false}).Complete(zerolog.InfoLevel, nil)
	require.NoError(t, err)
	require.NoError(t, s.Run(t.Context()))
	require.True(t, s.Insecure())
	s.GracefulStop()
}

func TestDisabledHTTP(t *testing.T) {
	s, err := (&HTTPServerConfig{HTTPEnabled: false}).Complete(zerolog.InfoLevel, nil)
	require.NoError(t, err)
	require.NoError(t, s.ListenAndServe())
	s.Close()
}

func TestValidateTLSConfig(t *testing.T) {
	t.Run("both options present is fine", func(t *testing.T) {
		require.NoError(t, (&GRPCServerConfig{TLSCertPath: "some path", TLSKeyPath: "some other path"}).validateTLSConfig())
	})
	t.Run("neither option present is fine", func(t *testing.T) {
		require.NoError(t, (&GRPCServerConfig{}).validateTLSConfig())
	})
	t.Run("one present but not the other returns an error", func(t *testing.T) {
		require.ErrorContains(t, (&GRPCServerConfig{TLSCertPath: "some path"}).validateTLSConfig(), "must provide both")
	})
}

// watchHealth opens a health Watch, a stream that never ends, on a buffered server with these limits,
// and returns a channel that gets the stream's error once the server ends it.
func watchHealth(t *testing.T, age, grace time.Duration) <-chan error {
	t.Helper()
	c := &GRPCServerConfig{Enabled: true, Network: BufferedNetwork, MaxConnAge: age, MaxConnAgeGrace: grace}
	s, err := c.Complete(zerolog.InfoLevel, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, health.NewServer()) })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() { _ = s.Run(ctx) }()
	t.Cleanup(s.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///buffnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return s.BufferedListener().DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		done <- err
	}()
	return done
}

func TestMaxConnAgeGraceEndsLongStream(t *testing.T) {
	t.Parallel()
	select {
	case err := <-watchHealth(t, 100*time.Millisecond, 200*time.Millisecond):
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("the stream outlived max age + grace")
	}
}

func TestMaxConnAgeWithoutGraceKeepsLongStream(t *testing.T) {
	t.Parallel()
	select {
	case err := <-watchHealth(t, 100*time.Millisecond, 0):
		t.Fatalf("the stream ended: %v", err)
	case <-time.After(time.Second):
	}
}

func TestMaxConnAgeGraceFlag(t *testing.T) {
	t.Parallel()
	c := &GRPCServerConfig{}
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterGRPCServerFlags(fs, c, "grpc", "grpc", ":50051", true)
	require.NoError(t, fs.Parse(nil))
	require.Zero(t, c.MaxConnAgeGrace, "no grace by default: grpc-go waits forever")
	require.NoError(t, fs.Parse([]string{"--grpc-max-conn-age-grace=1h"}))
	require.Equal(t, time.Hour, c.MaxConnAgeGrace)
}
