package util

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
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

func TestValidateSPIFFEConfig(t *testing.T) {
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "")

	t.Run("SPIFFE alone is fine", func(t *testing.T) {
		require.NoError(t, (&GRPCServerConfig{SPIFFE: true, SPIFFESocket: "unix:///run/spire/agent.sock"}).validateTLSConfig())
	})
	t.Run("SPIFFE and a TLS certificate are exclusive", func(t *testing.T) {
		config := &GRPCServerConfig{SPIFFE: true, SPIFFESocket: "unix:///run/spire/agent.sock", TLSCertPath: "cert", TLSKeyPath: "key", flagPrefix: "grpc"}
		require.ErrorContains(t, config.validateTLSConfig(), "--grpc-spiffe cannot be combined with --grpc-tls-cert-path")
	})
	t.Run("SPIFFE options without SPIFFE are an error", func(t *testing.T) {
		config := &GRPCServerConfig{SPIFFEAllowedIDs: []string{"spiffe://example.org/app"}, flagPrefix: "dispatch-cluster"}
		require.ErrorContains(t, config.validateTLSConfig(), "set --dispatch-cluster-spiffe")
	})
	t.Run("SPIFFE without a Workload API address is an error", func(t *testing.T) {
		require.ErrorContains(t, (&GRPCServerConfig{SPIFFE: true}).validateTLSConfig(), "set --grpc-spiffe-socket or SPIFFE_ENDPOINT_SOCKET")
	})
}
