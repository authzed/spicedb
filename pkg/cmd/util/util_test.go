package util

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestDisabledGRPC(t *testing.T) {
	s, err := (&GRPCServerConfig{Enabled: false}).Complete(zerolog.InfoLevel, nil)
	require.NoError(t, err)
	require.NoError(t, s.Listen(context.Background())())
	require.True(t, s.Insecure())
	s.GracefulStop()
}

func TestDisabledHTTP(t *testing.T) {
	s, err := (&HTTPServerConfig{HTTPEnabled: false}).Complete(zerolog.InfoLevel, nil)
	require.NoError(t, err)
	require.NoError(t, s.ListenAndServe())
	s.Close()
}
