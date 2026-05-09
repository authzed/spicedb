//go:build integration

// pkg/cmd/server/otel_integration_test.go
package server

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOTelIntegration_FullChain_EnvToProvider simulates the full
// DefaultPreRunE chain with OTel flags set and verifies the TracerProvider
// is non-nil in the command context after OTelPreRunE executes.
func TestOTelIntegration_FullChain_EnvToProvider(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	RegisterOTelFlags(cmd)
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("otel-provider", "otlpgrpc"))
	require.NoError(t, cmd.Flags().Set("otel-endpoint", "localhost:4317"))
	require.NoError(t, cmd.Flags().Set("otel-insecure", "true"))

	require.NoError(t, OTelPreRunE(cmd, nil))

	provider := OTelProviderFromContext(cmd.Context())
	require.NotNil(t, provider, "TracerProvider must be non-nil after OTelPreRunE")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ShutdownOTelProvider(ctx, provider)
	})
}

// TestOTelIntegration_ShutdownOnSignal verifies that ShutdownOTelProvider
// completes without error when called as a signal handler would call it.
func TestOTelIntegration_ShutdownOnSignal(t *testing.T) {
	mock := &mockShutdowner{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := ShutdownOTelProvider(ctx, mock)
	require.NoError(t, err)
	assert.True(t, mock.shutdownCalled, "expected Shutdown to be called")
	assert.True(t, mock.forceFlushCalled, "expected ForceFlush to be called")
}

// TestOTelIntegration_FlushBeforeShutdown verifies ForceFlush is called
// before Shutdown — flush-then-shutdown is the required ordering.
func TestOTelIntegration_FlushBeforeShutdown(t *testing.T) {
	callOrder := []string{}
	provider := &callOrderShutdowner{callLog: &callOrder}

	err := ShutdownOTelProvider(context.Background(), provider)
	require.NoError(t, err)
	require.Len(t, callOrder, 2)
	assert.Equal(t, "ForceFlush", callOrder[0],
		"ForceFlush must be called before Shutdown")
	assert.Equal(t, "Shutdown", callOrder[1])
}

// TestOTelIntegration_NoneProvider_SafeShutdown verifies that when
// OTelPreRunE ran with provider=none, the resulting nil provider can be
// passed to ShutdownOTelProvider without error.
func TestOTelIntegration_NoneProvider_SafeShutdown(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	RegisterOTelFlags(cmd)
	cmd.SetContext(context.Background())
	// otel-provider defaults to "none"

	require.NoError(t, OTelPreRunE(cmd, nil))

	provider := OTelProviderFromContext(cmd.Context())
	assert.Nil(t, provider)

	err := ShutdownOTelProvider(context.Background(), provider)
	assert.NoError(t, err)
}
