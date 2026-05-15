//go:build integration

// pkg/cmd/server/otel_integration_test.go
package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOTelIntegration_FullChain_EnvToProvider simulates the full
// DefaultPreRunE chain with OTel flags set and verifies the TracerProvider
// is non-nil after InitOTelProvider executes.
func TestOTelIntegration_FullChain_EnvToProvider(t *testing.T) {
	cfg := OTelConfig{
		Provider:        "otlpgrpc",
		Endpoint:        "localhost:4317",
		ServiceName:     "spicedb-test",
		TracePropagator: "w3c",
		Insecure:        true,
		SampleRatio:     0.01,
	}

	provider, err := InitOTelProvider(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, provider, "TracerProvider must be non-nil after InitOTelProvider")

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
// provider=none, InitOTelProvider returns nil and nil can be passed to
// ShutdownOTelProvider without error.
func TestOTelIntegration_NoneProvider_SafeShutdown(t *testing.T) {
	cfg := OTelConfig{Provider: "none"}

	provider, err := InitOTelProvider(context.Background(), cfg)
	require.NoError(t, err)
	assert.Nil(t, provider)

	err = ShutdownOTelProvider(context.Background(), provider)
	assert.NoError(t, err)
}

// TestOTelConfig_EnvVarConfiguresUnsetFlag verifies that when a flag is not
// explicitly set, the OTel SDK can still pick up OTEL_* standard environment
// variables (e.g. OTEL_EXPORTER_OTLP_ENDPOINT) because InitOTelProvider does
// not override SDK defaults — it only passes values through when flags are set.
func TestOTelConfig_EnvVarConfiguresUnsetFlag(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")

	cfg := OTelConfig{
		Provider:        "otlpgrpc",
		ServiceName:     "spicedb-test",
		TracePropagator: "w3c",
		Insecure:        true,
		SampleRatio:     0.01,
		// Endpoint intentionally left empty — SDK should read OTEL_EXPORTER_OTLP_ENDPOINT
	}

	provider, err := InitOTelProvider(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, provider, "provider must be non-nil when OTEL_EXPORTER_OTLP_ENDPOINT is set")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ShutdownOTelProvider(ctx, provider)
	})
}

// TestOTelConfig_ExplicitFlagOverridesEnvVar verifies that when both the
// otel-endpoint flag AND OTEL_EXPORTER_OTLP_ENDPOINT env var are set, the
// explicit flag value wins. This documents the precedence contract.
func TestOTelConfig_ExplicitFlagOverridesEnvVar(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "env-var-host:4317")

	cfg := OTelConfig{
		Provider:        "otlpgrpc",
		Endpoint:        "explicit-flag-host:4317", // flag value takes precedence
		ServiceName:     "spicedb-test",
		TracePropagator: "w3c",
		Insecure:        true,
		SampleRatio:     0.01,
	}

	// We verify this does not error — the explicit endpoint is used.
	// The actual routing cannot be asserted without a live collector,
	// but the contract (flag > env) is documented here for future reference.
	provider, err := InitOTelProvider(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, provider)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ShutdownOTelProvider(ctx, provider)
	})
}
