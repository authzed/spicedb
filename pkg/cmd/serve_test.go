package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cobrautil "github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/cmd/server"
)

// Run a serve command with specified command line args
// and call `assertConfig` with the merged configuration.
func RunServeTest(t *testing.T, args []string, assertConfig func(t *testing.T, mergedConfig *server.Config)) {
	config := server.NewConfigWithOptionsAndDefaults()
	cmd := NewServeCommand("spicedb", config)
	err := RegisterRootFlags(cmd)
	require.NoError(t, err)
	require.NoError(t, RegisterServeFlags(cmd, config))
	// Disable all metrics as they are singletons
	config.DispatchClusterMetricsEnabled = false
	config.DispatchClientMetricsEnabled = false
	config.DatastoreConfig.EnableDatastoreMetrics = false
	config.DispatchCacheConfig.Metrics = false
	config.ClusterDispatchCacheConfig.Metrics = false
	config.NamespaceCacheConfig.Metrics = false

	cmd.SetArgs(args)

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(cmd.Context())
		t.Cleanup(cancel)

		_, err := config.Complete(ctx)
		if err != nil {
			return err
		}
		assertConfig(t, config)
		return nil
	}
	require.NoError(t, cmd.Execute())
}

func prepareTempConfigDir(t *testing.T) string {
	testdir := t.TempDir()
	t.Chdir(testdir)
	return testdir
}

func prepareTempConfigFile(t *testing.T, config string) {
	confdir := prepareTempConfigDir(t)
	confFile, err := os.Create(filepath.Join(confdir, "spicedb.env"))
	require.NoError(t, err)
	_, err = confFile.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, confFile.Close())
	prevEnv := os.Environ()
	restore := func() {
		restoreEnv(prevEnv)
	}
	t.Cleanup(restore)
}

func restoreEnv(prevEnv []string) {
	os.Clearenv()
	for _, line := range prevEnv {
		envvar := strings.SplitN(line, "=", 2)
		os.Setenv(envvar[0], envvar[1])
	}
}

func TestDefaultConfig(t *testing.T) {
	flags := []string{
		"--grpc-preshared-key", "some_key",
	}
	RunServeTest(t, flags, func(t *testing.T, mergedConfig *server.Config) {
		require.True(t, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, ":50051", mergedConfig.GRPCServer.Address)
		require.False(t, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, ":8443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(1024), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}

func TestFlagsAreParsed(t *testing.T) {
	flags := []string{
		"--grpc-preshared-key", "some_key",
		"--grpc-addr", "127.0.0.1:9051",
		"--http-enabled",
		"--http-addr", "127.0.0.1:9443",
		"--datastore-engine", "memory",
		"--datastore-watch-buffer-length", "9000",
	}
	RunServeTest(t, flags, func(t *testing.T, mergedConfig *server.Config) {
		require.True(t, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:9051", mergedConfig.GRPCServer.Address)
		require.True(t, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, "127.0.0.1:9443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(9000), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}

func TestEnvVarsAreParsed(t *testing.T) {
	t.Setenv("SPICEDB_GRPC_PRESHARED_KEY", "some_key")
	t.Setenv("SPICEDB_GRPC_ADDR", "127.0.0.1:10051")
	t.Setenv("SPICEDB_HTTP_ENABLED", "1")
	t.Setenv("SPICEDB_HTTP_ADDR", "127.0.0.1:10443")
	t.Setenv("SPICEDB_DATASTORE_ENGINE", "memory")
	t.Setenv("SPICEDB_DATASTORE_WATCH_BUFFER_LENGTH", "10000")
	flags := []string{}
	RunServeTest(t, flags, func(t *testing.T, mergedConfig *server.Config) {
		require.True(t, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:10051", mergedConfig.GRPCServer.Address)
		require.True(t, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, "127.0.0.1:10443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(10000), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}

func TestConfigFileValuesAreParsed(t *testing.T) {
	config := `
	SPICEDB_GRPC_PRESHARED_KEY=some_key
	SPICEDB_GRPC_ADDR=127.0.0.1:11051
	SPICEDB_HTTP_ENABLED=1
	SPICEDB_HTTP_ADDR=127.0.0.1:11443
	SPICEDB_DATASTORE_ENGINE=memory
	SPICEDB_DATASTORE_WATCH_BUFFER_LENGTH=11000`
	prepareTempConfigFile(t, config)
	flags := []string{}
	RunServeTest(t, flags, func(t *testing.T, mergedConfig *server.Config) {
		require.True(t, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:11051", mergedConfig.GRPCServer.Address)
		require.True(t, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, "127.0.0.1:11443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(11000), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}

func TestConfigsAreMerged(t *testing.T) {
	config := `
	SPICEDB_GRPC_PRESHARED_KEY=some_key
	SPICEDB_GRPC_ADDR=127.0.0.1:12051`
	prepareTempConfigFile(t, config)

	t.Setenv("SPICEDB_HTTP_ENABLED", "1")
	t.Setenv("SPICEDB_HTTP_ADDR", "127.0.0.1:12443")

	flags := []string{
		"--datastore-engine", "memory",
		"--datastore-watch-buffer-length", "12000",
	}
	RunServeTest(t, flags, func(t *testing.T, mergedConfig *server.Config) {
		require.True(t, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:12051", mergedConfig.GRPCServer.Address)
		require.True(t, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, "127.0.0.1:12443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(12000), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}

func TestConfigPrecedence(t *testing.T) {
	// Config file has lowest precedence (over default values)
	config := `
	SPICEDB_GRPC_PRESHARED_KEY=some_key
	SPICEDB_GRPC_ADDR=127.0.0.1:21051
	SPICEDB_HTTP_ENABLED=1
	SPICEDB_HTTP_ADDR=127.0.0.1:21443
	SPICEDB_DATASTORE_ENGINE=memory
	SPICEDB_DATASTORE_WATCH_BUFFER_LENGTH=21000`
	prepareTempConfigFile(t, config)

	// Env variables override config file
	t.Setenv("SPICEDB_HTTP_ENABLED", "1")
	t.Setenv("SPICEDB_HTTP_ADDR", "127.0.0.1:22443")
	t.Setenv("SPICEDB_DATASTORE_ENGINE", "memory")
	t.Setenv("SPICEDB_DATASTORE_WATCH_BUFFER_LENGTH", "22000")

	// command line flags override everything
	flags := []string{
		"--datastore-engine", "memory",
		"--datastore-watch-buffer-length", "23000",
	}
	RunServeTest(t, flags, func(t *testing.T, mergedConfig *server.Config) {
		require.True(t, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:21051", mergedConfig.GRPCServer.Address)
		require.True(t, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, "127.0.0.1:22443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(23000), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}

// runOTelProviderTest wires up a real serve command (so all flags are
// registered) but replaces PreRunE with a minimal stack that only runs
// SyncViperDotEnvPreRunE followed by inferOTelProviderFromEnvPreRunE. This
// means no actual OTel exporter connection is ever opened, keeping the tests
// hermetic. The resolved value of --otel-provider is returned.
func runOTelProviderTest(t *testing.T, args []string, envVars map[string]string) string {
	t.Helper()

	for k, v := range envVars {
		t.Setenv(k, v)
	}

	config := server.NewConfigWithOptionsAndDefaults()
	cmd := NewServeCommand("spicedb", config)
	require.NoError(t, RegisterRootFlags(cmd))
	require.NoError(t, RegisterServeFlags(cmd, config))

	// Disable metrics singletons to avoid test pollution.
	config.DispatchClusterMetricsEnabled = false
	config.DispatchClientMetricsEnabled = false
	config.DatastoreConfig.EnableDatastoreMetrics = false
	config.DispatchCacheConfig.Metrics = false
	config.ClusterDispatchCacheConfig.Metrics = false
	config.NamespaceCacheConfig.Metrics = false

	var resolvedProvider string

	// Replace PreRunE with only the two hooks we care about:
	//   1. SyncViper – so SPICEDB_* env vars (and .env file) are applied.
	//   2. inferOTelProviderFromEnvPreRunE – the logic under test.
	// cobraotel.RunE is deliberately excluded so no collector connection
	// is ever opened.
	cmd.PreRunE = func(cmd *cobra.Command, cmdArgs []string) error {
		syncViper := cobrautil.SyncViperPreRunE("spicedb")
		if err := syncViper(cmd, cmdArgs); err != nil {
			return err
		}
		infer := server.InferOTelProviderFromEnvPreRunE()
		return infer(cmd, cmdArgs)
	}

	// RunE just reads the flag after PreRunE has fired.
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		f := cmd.Flags().Lookup("otel-provider")
		if f != nil {
			resolvedProvider = f.Value.String()
		}
		return nil
	}

	baseArgs := []string{"--grpc-preshared-key", "test-key"}
	cmd.SetArgs(append(baseArgs, args...))
	require.NoError(t, cmd.Execute())

	return resolvedProvider
}

// TestOTelEnvOnlyEnablesTracing verifies that setting a standard OTEL_*
// environment variable (without any SpiceDB flags) causes the provider to be
// inferred as "otlpgrpc".
func TestOTelEnvOnlyEnablesTracing(t *testing.T) {
	provider := runOTelProviderTest(t, nil, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "localhost:4317",
	})
	require.Equal(t, "otlpgrpc", provider)
}

// TestOTelTracesEndpointEnablesTracing covers the OTEL_EXPORTER_OTLP_TRACES_ENDPOINT variant.
func TestOTelTracesEndpointEnablesTracing(t *testing.T) {
	provider := runOTelProviderTest(t, nil, map[string]string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "localhost:4317",
	})
	require.Equal(t, "otlpgrpc", provider)
}

// TestOTelTracesExporterEnablesTracing covers the OTEL_TRACES_EXPORTER variant.
func TestOTelTracesExporterEnablesTracing(t *testing.T) {
	provider := runOTelProviderTest(t, nil, map[string]string{
		"OTEL_TRACES_EXPORTER": "otlp",
	})
	require.Equal(t, "otlpgrpc", provider)
}

// TestOTelFlagOnlyUnchanged verifies that when only SpiceDB flags are used,
// they are passed through unchanged.
func TestOTelFlagOnlyUnchanged(t *testing.T) {
	provider := runOTelProviderTest(t,
		[]string{"--otel-provider", "otlphttp"},
		nil,
	)
	require.Equal(t, "otlphttp", provider)
}

// TestOTelExplicitNoneDisablesTracing verifies that --otel-provider=none
// overrides any OTEL_* environment variables; tracing stays disabled.
func TestOTelExplicitNoneDisablesTracing(t *testing.T) {
	provider := runOTelProviderTest(t,
		[]string{"--otel-provider", "none"},
		map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": "localhost:4317",
		},
	)
	require.Equal(t, "none", provider)
}

// TestOTelNoConfigDefaultsToNone verifies the baseline: with no flags and no
// OTEL_* env vars, the provider remains "none" and tracing is disabled.
func TestOTelNoConfigDefaultsToNone(t *testing.T) {
	// Scrub any OTEL_* vars that might be set in the test environment.
	os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	os.Unsetenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	os.Unsetenv("OTEL_TRACES_EXPORTER")

	provider := runOTelProviderTest(t, nil, nil)
	require.Equal(t, "none", provider)
}

