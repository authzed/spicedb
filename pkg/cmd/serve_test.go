package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	require.Nil(t, err)
	require.Nil(t, RegisterServeFlags(cmd, config))
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
	require.Nil(t, cmd.Execute())
}

func prepareTempConfigDir(t *testing.T) string {
	workdir, err := os.Getwd()
	require.Nil(t, err)

	testdir := t.TempDir()
	require.Nil(t, os.Chdir(testdir))

	t.Cleanup(func() {
		err = os.Chdir(workdir)
		require.Nil(t, err)
	})

	return testdir
}

func prepareTempConfigFile(t *testing.T, config string) {
	confdir := prepareTempConfigDir(t)
	confFile, err := os.Create(filepath.Join(confdir, "spicedb.env"))
	require.Nil(t, err)
	_, err = confFile.WriteString(config)
	require.Nil(t, err)
	require.Nil(t, confFile.Close())
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
		require.Equal(t, true, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, ":50051", mergedConfig.GRPCServer.Address)
		require.Equal(t, false, mergedConfig.HTTPGateway.HTTPEnabled)
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
		require.Equal(t, true, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:9051", mergedConfig.GRPCServer.Address)
		require.Equal(t, true, mergedConfig.HTTPGateway.HTTPEnabled)
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
		require.Equal(t, true, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:10051", mergedConfig.GRPCServer.Address)
		require.Equal(t, true, mergedConfig.HTTPGateway.HTTPEnabled)
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
		require.Equal(t, true, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:11051", mergedConfig.GRPCServer.Address)
		require.Equal(t, true, mergedConfig.HTTPGateway.HTTPEnabled)
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
		require.Equal(t, true, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:12051", mergedConfig.GRPCServer.Address)
		require.Equal(t, true, mergedConfig.HTTPGateway.HTTPEnabled)
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
		require.Equal(t, true, mergedConfig.GRPCServer.Enabled)
		require.Equal(t, "127.0.0.1:21051", mergedConfig.GRPCServer.Address)
		require.Equal(t, true, mergedConfig.HTTPGateway.HTTPEnabled)
		require.Equal(t, "127.0.0.1:22443", mergedConfig.HTTPGateway.HTTPAddress)
		require.Equal(t, "memory", mergedConfig.DatastoreConfig.Engine)
		require.Equal(t, uint16(23000), mergedConfig.DatastoreConfig.WatchBufferLength)
	})
}
