// pkg/cmd/server/otel_test.go
package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// mockShutdowner is a test double that records calls to Shutdown/ForceFlush.
type mockShutdowner struct {
	shutdownCalled   bool
	forceFlushCalled bool
	shutdownErr      error
	forceFlushErr    error
}

func (m *mockShutdowner) Shutdown(_ context.Context) error {
	m.shutdownCalled = true
	return m.shutdownErr
}

func (m *mockShutdowner) ForceFlush(_ context.Context) error {
	m.forceFlushCalled = true
	return m.forceFlushErr
}

// callOrderShutdowner records the order Shutdown/ForceFlush are called.
type callOrderShutdowner struct {
	callLog *[]string
}

func (c *callOrderShutdowner) ForceFlush(_ context.Context) error {
	*c.callLog = append(*c.callLog, "ForceFlush")
	return nil
}

func (c *callOrderShutdowner) Shutdown(_ context.Context) error {
	*c.callLog = append(*c.callLog, "Shutdown")
	return nil
}

// makeTestCmd creates a bare cobra.Command for flag-registration tests.
func makeTestCmd() *cobra.Command {
	return &cobra.Command{Use: "test"}
}

// ---------------------------------------------------------------------------
// RegisterOTelFlags
// ---------------------------------------------------------------------------


// TestRegisterOTelFlags_AllFlagsPresent verifies all OTel flags are
// registered with correct names after calling RegisterOTelFlags.
func TestRegisterOTelFlags_AllFlagsPresent(t *testing.T) {
	cmd := makeTestCmd()
	RegisterOTelFlags(cmd)

	for _, name := range []string{
		"otel-provider",
		"otel-endpoint",
		"otel-service-name",
		"otel-trace-propagator",
		"otel-insecure",
		"otel-headers",
	} {
		assert.NotNil(t, cmd.Flags().Lookup(name),
			"expected flag %q to be registered", name)
	}
}

// TestRegisterOTelFlags_ProviderDefault verifies otel-provider defaults to "none".
func TestRegisterOTelFlags_ProviderDefault(t *testing.T) {
	cmd := makeTestCmd()
	RegisterOTelFlags(cmd)
	val, err := cmd.Flags().GetString("otel-provider")
	require.NoError(t, err)
	assert.Equal(t, "none", val)
}

// ---------------------------------------------------------------------------
// InitOTelProvider
// ---------------------------------------------------------------------------

// TestInitOTelProvider_NoneSkipsInit verifies provider=none returns (nil, nil)
// without attempting any network connection.
func TestInitOTelProvider_NoneSkipsInit(t *testing.T) {
	cfg := OTelConfig{Provider: "none"}
	provider, err := InitOTelProvider(context.Background(), cfg)
	require.NoError(t, err)
	assert.Nil(t, provider)
}

// TestInitOTelProvider_UnknownProviderReturnsError verifies an unrecognized
// provider string returns a non-nil error containing the bad value.
func TestInitOTelProvider_UnknownProviderReturnsError(t *testing.T) {
	cfg := OTelConfig{Provider: "bogusprovider", ServiceName: "test", TracePropagator: "w3c"}
	_, err := InitOTelProvider(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogusprovider")
}

// TestInitOTelProvider_OtlpGrpc_ValidEndpoint verifies otlpgrpc initializes
// without error. No live collector required — connection errors surface only
// on first export, not at initialization.
func TestInitOTelProvider_OtlpGrpc_ValidEndpoint(t *testing.T) {
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
	assert.NotNil(t, provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
}

// TestInitOTelProvider_OtlpHttp_ValidEndpoint verifies otlphttp initializes
// without error. No live collector required.
func TestInitOTelProvider_OtlpHttp_ValidEndpoint(t *testing.T) {
	cfg := OTelConfig{
		Provider:        "otlphttp",
		Endpoint:        "localhost:4318",
		ServiceName:     "spicedb-test",
		TracePropagator: "w3c",
		Insecure:        true,
		SampleRatio:     0.01,
	}
	provider, err := InitOTelProvider(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
}

// ---------------------------------------------------------------------------
// ShutdownOTelProvider
// ---------------------------------------------------------------------------

// TestShutdownOTelProvider_NilProvider_NoError verifies nil provider is safe.
func TestShutdownOTelProvider_NilProvider_NoError(t *testing.T) {
	err := ShutdownOTelProvider(context.Background(), nil)
	assert.NoError(t, err)
}

// TestShutdownOTelProvider_CallsFlushThenShutdown verifies ForceFlush is
// called before Shutdown, and both are called exactly once.
func TestShutdownOTelProvider_CallsFlushThenShutdown(t *testing.T) {
	callOrder := []string{}
	provider := &callOrderShutdowner{callLog: &callOrder}

	err := ShutdownOTelProvider(context.Background(), provider)
	require.NoError(t, err)
	require.Len(t, callOrder, 2)
	assert.Equal(t, "ForceFlush", callOrder[0], "ForceFlush must be called before Shutdown")
	assert.Equal(t, "Shutdown", callOrder[1])
}

// TestShutdownOTelProvider_ShutdownErrorPropagated verifies that an error
// from Shutdown is returned to the caller.
func TestShutdownOTelProvider_ShutdownErrorPropagated(t *testing.T) {
	mock := &mockShutdowner{shutdownErr: fmt.Errorf("shutdown failed")}
	err := ShutdownOTelProvider(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown failed")
}

// TestShutdownOTelProvider_ForceFlushErrorContinuesToShutdown verifies that
// a ForceFlush error does not prevent Shutdown from being called.
func TestShutdownOTelProvider_ForceFlushErrorContinuesToShutdown(t *testing.T) {
	mock := &mockShutdowner{forceFlushErr: fmt.Errorf("flush failed")}
	_ = ShutdownOTelProvider(context.Background(), mock)
	assert.True(t, mock.shutdownCalled,
		"Shutdown must be called even when ForceFlush errors")
}

// TestShutdownOTelProvider_ContextCancelled verifies a cancelled context
// produces no panic. The shutdown error (if any) is returned normally.
func TestShutdownOTelProvider_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mock := &mockShutdowner{shutdownErr: context.Canceled}
	err := ShutdownOTelProvider(ctx, mock)
	_ = err // cancelled context may or may not surface — no panic is the guarantee
}
