package mysql

import (
	"fmt"
	"time"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func NewCommand(programName string) *cobra.Command {
	crdbCmd := &cobra.Command{
		Use:     "mysql",
		Aliases: []string{"mariadb", "vitess"},
		Short:   "Perform operations on data stored in MySQL variants",
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
			head, err := migrations.Manager.HeadRevision()
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
	cmd.Flags().String("mysql-uri", "mysql://mysql:password@localhost:5432/spicedb", "connection string in URI format")
	cmd.Flags().String("mysql-table-prefix", "", "prefix to include in all table names")
	cmd.Flags().Uint64("backfill-batch-size", 1000, "batch size used when backfilling data")
	cmd.Flags().Duration("timeout", 1*time.Hour, "maximum execution duration for an individual migration")
}

func ExecMigrationRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	revision := args[0]
	if revision == "head" {
		head, err := migrations.Manager.HeadRevision()
		if err != nil {
			return fmt.Errorf("unable to compute head revision: %w", err)
		}
		revision = head
	}

	var credsProvider datastore.CredentialsProvider
	if providerName := cobrautil.MustGetString(cmd, "datastore-credentials-provider-name"); providerName != "" {
		var err error
		credsProvider, err = datastore.NewCredentialsProvider(ctx, providerName)
		if err != nil {
			return err
		}
	}

	migrationDriver, err := migrations.NewMySQLDriverFromDSN(
		cobrautil.MustGetStringExpanded(cmd, "mysql-uri"),
		cobrautil.MustGetStringExpanded(cmd, "mysql-table-prefix"),
		credsProvider,
	)
	if err != nil {
		return fmt.Errorf("unable to create mysql migration driver: %w", err)
	}

	log.Ctx(ctx).Info().Str("target", revision).Msg("executing migrations")
	return migrate.RunMigration(
		ctx,
		migrationDriver,
		migrations.Manager,
		revision,
		cobrautil.MustGetDuration(cmd, "timeout"),
		cobrautil.MustGetUint64(cmd, "backfill-batch-size"),
	)
}
