package cockroachdb

import (
	"fmt"
	"time"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/migrate"
)

func NewCommand(programName string) *cobra.Command {
	crdbCmd := &cobra.Command{
		Use:     "cockroachdb",
		Aliases: []string{"cockroach", "crdb"},
		Short:   "Perform operations on data stored in CockroachDB",
		GroupID: "datastores",
		Hidden:  false,
	}
	migrationsCmd := NewMigrationCommand(programName)
	crdbCmd.AddCommand(migrationsCmd)

	return crdbCmd
}

func NewMigrationCommand(programName string) *cobra.Command {
	migrationsCmd := &cobra.Command{
		Use:   "migrations",
		Short: "Perform migrations and schema changes",
	}

	headCmd := &cobra.Command{
		Use:   "head",
		Short: "Print the latest migration",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			head, err := migrations.CRDBMigrations.HeadRevision()
			if err != nil {
				return fmt.Errorf("unable to compute head revision: %w", err)
			}
			_, err = fmt.Println(head)
			return err
		},
	}
	migrationsCmd.AddCommand(headCmd)

	execCmd := &cobra.Command{
		Use:   "exec <target migration>",
		Short: "Execute all migrations up to and including the provided migration",
		Args:  cobra.ExactArgs(1),
		RunE:  ExecMigrationRunE,
	}
	migrationsCmd.AddCommand(execCmd)
	RegisterMigrationExecFlags(execCmd)

	return migrationsCmd
}

func RegisterMigrationExecFlags(cmd *cobra.Command) {
	cmd.Flags().String("crdb-uri", "postgres://roach:password@localhost:5432/spicedb", "connection string in URI format")
	cmd.Flags().Uint64("backfill-batch-size", 1000, "batch size used when backfilling data")
	cmd.Flags().Duration("timeout", 1*time.Hour, "maximum execution duration for an individual migration")
}

func ExecMigrationRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	revision := args[0]
	if revision == "head" {
		head, err := migrations.CRDBMigrations.HeadRevision()
		if err != nil {
			return fmt.Errorf("unable to compute head revision: %w", err)
		}
		revision = head
	}

	log.Ctx(ctx).Info().Str("target", revision).Msg("executing migrations")

	migrationDriver, err := migrations.NewCRDBDriver(cobrautil.MustGetStringExpanded(cmd, "crdb-uri"))
	if err != nil {
		return fmt.Errorf("unable to create cockroachdb migration driver: %w", err)
	}

	return migrate.RunMigration(
		cmd.Context(),
		migrationDriver,
		migrations.CRDBMigrations,
		revision,
		cobrautil.MustGetDuration(cmd, "timeout"),
		cobrautil.MustGetUint64(cmd, "backfill-batch-size"),
	)
}
