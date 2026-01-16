//go:build cgo

package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// Run a postgres-fdw command with specified command line args
// and call `assertConfig` with the merged configuration.
func RunPostgresFDWTest(t *testing.T, args []string, assertConfig func(t *testing.T, mergedConfig *PostgresFDWConfig)) {
	config := new(PostgresFDWConfig)
	cmd := NewPostgresFDWCommand("spicedb", config)
	err := RegisterRootFlags(cmd)
	require.NoError(t, err)
	require.NoError(t, RegisterPostgresFDWFlags(cmd, config))

	cmd.SetArgs(args)

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		assertConfig(t, config)
		return nil
	}
	require.NoError(t, cmd.Execute())
}

func preparePostgresFDWTempConfigFile(t *testing.T, config string) {
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

func TestPostgresFDWDefaultConfig(t *testing.T) {
	flags := []string{
		"--spicedb-access-token-secret", "test_token",
		"--postgres-access-token-secret", "postgres_token",
	}
	RunPostgresFDWTest(t, flags, func(t *testing.T, mergedConfig *PostgresFDWConfig) {
		require.Equal(t, "localhost:50051", mergedConfig.SpiceDBEndpoint)
		require.Equal(t, "test_token", mergedConfig.SecureSpiceDBAccessToken)
		require.False(t, mergedConfig.SpiceDBInsecure)
		require.Equal(t, ":5432", mergedConfig.PostgresEndpoint)
		require.Equal(t, "postgres", mergedConfig.PostgresUsername)
		require.Equal(t, "postgres_token", mergedConfig.SecureAccessToken)
	})
}

func TestPostgresFDWFlagsAreParsed(t *testing.T) {
	flags := []string{
		"--spicedb-api-endpoint", "127.0.0.1:50052",
		"--spicedb-access-token-secret", "test_token",
		"--spicedb-insecure",
		"--postgres-endpoint", "127.0.0.1:5433",
		"--postgres-username", "customuser",
		"--postgres-access-token-secret", "postgres_token",
		"--shutdown-grace-period", "10s",
	}
	RunPostgresFDWTest(t, flags, func(t *testing.T, mergedConfig *PostgresFDWConfig) {
		require.Equal(t, "127.0.0.1:50052", mergedConfig.SpiceDBEndpoint)
		require.Equal(t, "test_token", mergedConfig.SecureSpiceDBAccessToken)
		require.True(t, mergedConfig.SpiceDBInsecure)
		require.Equal(t, "127.0.0.1:5433", mergedConfig.PostgresEndpoint)
		require.Equal(t, "customuser", mergedConfig.PostgresUsername)
		require.Equal(t, "postgres_token", mergedConfig.SecureAccessToken)
		require.Equal(t, "10s", mergedConfig.ShutdownGracePeriod.String())
	})
}

func TestPostgresFDWEnvVarsAreParsed(t *testing.T) {
	t.Setenv("SPICEDB_SPICEDB_API_ENDPOINT", "127.0.0.1:50053")
	t.Setenv("SPICEDB_SPICEDB_ACCESS_TOKEN_SECRET", "test_token")
	t.Setenv("SPICEDB_SPICEDB_INSECURE", "true")
	t.Setenv("SPICEDB_POSTGRES_ENDPOINT", "127.0.0.1:5434")
	t.Setenv("SPICEDB_POSTGRES_USERNAME", "envuser")
	t.Setenv("SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET", "postgres_token")
	t.Setenv("SPICEDB_SHUTDOWN_GRACE_PERIOD", "15s")
	flags := []string{}
	RunPostgresFDWTest(t, flags, func(t *testing.T, mergedConfig *PostgresFDWConfig) {
		require.Equal(t, "127.0.0.1:50053", mergedConfig.SpiceDBEndpoint)
		require.Equal(t, "test_token", mergedConfig.SecureSpiceDBAccessToken)
		require.True(t, mergedConfig.SpiceDBInsecure)
		require.Equal(t, "127.0.0.1:5434", mergedConfig.PostgresEndpoint)
		require.Equal(t, "envuser", mergedConfig.PostgresUsername)
		require.Equal(t, "postgres_token", mergedConfig.SecureAccessToken)
		require.Equal(t, "15s", mergedConfig.ShutdownGracePeriod.String())
	})
}

func TestPostgresFDWConfigFileValuesAreParsed(t *testing.T) {
	config := `
	SPICEDB_SPICEDB_API_ENDPOINT=127.0.0.1:50054
	SPICEDB_SPICEDB_ACCESS_TOKEN_SECRET=test_token
	SPICEDB_SPICEDB_INSECURE=true
	SPICEDB_POSTGRES_ENDPOINT=127.0.0.1:5435
	SPICEDB_POSTGRES_USERNAME=fileuser
	SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET=postgres_token
	SPICEDB_SHUTDOWN_GRACE_PERIOD=20s`
	preparePostgresFDWTempConfigFile(t, config)
	flags := []string{}
	RunPostgresFDWTest(t, flags, func(t *testing.T, mergedConfig *PostgresFDWConfig) {
		require.Equal(t, "127.0.0.1:50054", mergedConfig.SpiceDBEndpoint)
		require.Equal(t, "test_token", mergedConfig.SecureSpiceDBAccessToken)
		require.True(t, mergedConfig.SpiceDBInsecure)
		require.Equal(t, "127.0.0.1:5435", mergedConfig.PostgresEndpoint)
		require.Equal(t, "fileuser", mergedConfig.PostgresUsername)
		require.Equal(t, "postgres_token", mergedConfig.SecureAccessToken)
		require.Equal(t, "20s", mergedConfig.ShutdownGracePeriod.String())
	})
}

func TestPostgresFDWConfigsAreMerged(t *testing.T) {
	config := `
	SPICEDB_SPICEDB_API_ENDPOINT=127.0.0.1:50055
	SPICEDB_SPICEDB_ACCESS_TOKEN_SECRET=test_token`
	preparePostgresFDWTempConfigFile(t, config)

	t.Setenv("SPICEDB_POSTGRES_ENDPOINT", "127.0.0.1:5436")
	t.Setenv("SPICEDB_POSTGRES_USERNAME", "mergeduser")
	t.Setenv("SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET", "postgres_token")

	flags := []string{
		"--shutdown-grace-period", "25s",
	}
	RunPostgresFDWTest(t, flags, func(t *testing.T, mergedConfig *PostgresFDWConfig) {
		require.Equal(t, "127.0.0.1:50055", mergedConfig.SpiceDBEndpoint)
		require.Equal(t, "test_token", mergedConfig.SecureSpiceDBAccessToken)
		require.Equal(t, "127.0.0.1:5436", mergedConfig.PostgresEndpoint)
		require.Equal(t, "mergeduser", mergedConfig.PostgresUsername)
		require.Equal(t, "postgres_token", mergedConfig.SecureAccessToken)
		require.Equal(t, "25s", mergedConfig.ShutdownGracePeriod.String())
	})
}

func TestPostgresFDWConfigPrecedence(t *testing.T) {
	// Config file has lowest precedence (over default values)
	config := `
	SPICEDB_SPICEDB_API_ENDPOINT=127.0.0.1:50056
	SPICEDB_SPICEDB_ACCESS_TOKEN_SECRET=test_token_file
	SPICEDB_POSTGRES_ENDPOINT=127.0.0.1:5437
	SPICEDB_POSTGRES_USERNAME=fileuser
	SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET=postgres_token_file
	SPICEDB_SHUTDOWN_GRACE_PERIOD=30s`
	preparePostgresFDWTempConfigFile(t, config)

	// Env variables override config file
	t.Setenv("SPICEDB_POSTGRES_ENDPOINT", "127.0.0.1:5438")
	t.Setenv("SPICEDB_POSTGRES_USERNAME", "envuser")
	t.Setenv("SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET", "postgres_token_env")
	t.Setenv("SPICEDB_SHUTDOWN_GRACE_PERIOD", "35s")

	// command line flags override everything
	flags := []string{
		"--postgres-username", "flaguser",
		"--shutdown-grace-period", "40s",
	}
	RunPostgresFDWTest(t, flags, func(t *testing.T, mergedConfig *PostgresFDWConfig) {
		require.Equal(t, "127.0.0.1:50056", mergedConfig.SpiceDBEndpoint)
		require.Equal(t, "test_token_file", mergedConfig.SecureSpiceDBAccessToken)
		require.Equal(t, "127.0.0.1:5438", mergedConfig.PostgresEndpoint)
		require.Equal(t, "flaguser", mergedConfig.PostgresUsername)
		require.Equal(t, "postgres_token_env", mergedConfig.SecureAccessToken)
		require.Equal(t, "40s", mergedConfig.ShutdownGracePeriod.String())
	})
}

func TestPostgresFDWCompleteValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("missing SpiceDB endpoint", func(t *testing.T) {
		config := &PostgresFDWConfig{
			SpiceDBEndpoint:          "",
			SecureSpiceDBAccessToken: "test_token",
			PostgresEndpoint:         ":5432",
			PostgresUsername:         "postgres",
			SecureAccessToken:        "postgres_token",
		}
		_, err := config.Complete(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing SpiceDB endpoint")
	})

	t.Run("missing SpiceDB access token", func(t *testing.T) {
		config := &PostgresFDWConfig{
			SpiceDBEndpoint:          "localhost:50051",
			SecureSpiceDBAccessToken: "",
			PostgresEndpoint:         ":5432",
			PostgresUsername:         "postgres",
			SecureAccessToken:        "postgres_token",
		}
		_, err := config.Complete(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing SpiceDB access token")
	})

	t.Run("missing Postgres access token", func(t *testing.T) {
		config := &PostgresFDWConfig{
			SpiceDBEndpoint:          "localhost:50051",
			SecureSpiceDBAccessToken: "test_token",
			PostgresEndpoint:         ":5432",
			PostgresUsername:         "postgres",
			SecureAccessToken:        "",
		}
		_, err := config.Complete(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing Postgres secure access token")
	})

	t.Run("missing Postgres username", func(t *testing.T) {
		config := &PostgresFDWConfig{
			SpiceDBEndpoint:          "localhost:50051",
			SecureSpiceDBAccessToken: "test_token",
			PostgresEndpoint:         ":5432",
			PostgresUsername:         "",
			SecureAccessToken:        "postgres_token",
		}
		_, err := config.Complete(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing Postgres username")
	})
}
