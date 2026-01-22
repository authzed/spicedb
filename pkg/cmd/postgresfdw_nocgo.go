//go:build !cgo

package cmd

import (
	"errors"
	"time"

	"github.com/spf13/cobra"
)

// PostgresFDWConfig is the configuration for the Postgres FDW command (stub for non-CGO builds)
type PostgresFDWConfig struct {
	// SpiceDB connection config
	SpiceDBEndpoint          string
	SecureSpiceDBAccessToken string
	SpiceDBInsecure          bool

	// Postgres FDW server config
	PostgresEndpoint    string
	PostgresUsername    string
	SecureAccessToken   string
	ShutdownGracePeriod time.Duration
}

// NewPostgresFDWCommand returns a command that errors when CGO is not enabled
func NewPostgresFDWCommand(programName string, config *PostgresFDWConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "postgres-fdw",
		Short: "serve a Postgres Foreign Data Wrapper for SpiceDB (EXPERIMENTAL)",
		Long:  "EXPERIMENTAL: Serves a Postgres-compatible interface for querying SpiceDB data using foreign data wrappers. This feature requires CGO to be enabled during compilation.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("postgres-fdw command requires CGO to be enabled during compilation")
		},
	}
}

// RegisterPostgresFDWFlags is a no-op when CGO is not enabled
func RegisterPostgresFDWFlags(cmd *cobra.Command, config *PostgresFDWConfig) error {
	return nil
}
