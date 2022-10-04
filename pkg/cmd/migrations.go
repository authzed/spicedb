package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/cmd/server"
)

func RegisterMigrationFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "postgres", fmt.Sprintf(`type of migrations to export (%s)`, "postgres"))
	cmd.Flags().String("dir", "migrations", "directory to write migration files")
}

func NewMigrationCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "migrations",
		Short:   "Inspect, compute, and export datastore migrations.",
		Long:    "Computes and exports datastore migrations for auditing or integration with external tools. Most users should use \"spicedb migrate\" instead.",
		PreRunE: server.DefaultPreRunE(programName),
		RunE:    migrationRun,
		Args:    cobra.MaximumNArgs(1),
	}
}

func migrationRun(cmd *cobra.Command, args []string) error {
	datastoreEngine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
	directory := cobrautil.MustGetStringExpanded(cmd, "dir")

	if err := os.MkdirAll(directory, 0o600); err != nil {
		return err
	}

	switch datastoreEngine {
	case postgres.Engine:
		migs := migrations.PostgresMigrations
		migs.Sort()
		epoch := time.Now().Unix()
		for i, m := range migs {
			file := filepath.Join(directory, m.FileName(fmt.Sprintf("%d", epoch+int64(i))))
			if err := os.WriteFile(file, m.Bytes(), 0o600); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("migration export not supported for engine: %s", datastoreEngine)
	}
	return nil
}
