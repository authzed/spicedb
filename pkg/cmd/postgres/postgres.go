package postgres

import (
	"fmt"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func NewPostgresCommand(programName string) *cobra.Command {
	pgCmd := &cobra.Command{
		Use:     "postgres",
		Aliases: []string{"pg", "postgresql"},
		Short:   "Perform operations on data stored in PostgreSQL",
		GroupID: "datastores",
		Hidden:  false,
	}

	migrationsCmd := &cobra.Command{
		Use:   "migrations",
		Short: "Perform migrations and schema changes",
	}

	headCmd := &cobra.Command{
		Use:     "head",
		Short:   "Print the latest migration",
		Args:    cobra.ExactArgs(0),
		PreRunE: nil, // TODO
		RunE: func(cmd *cobra.Command, args []string) error {
			head, err := migrations.DatabaseMigrations.HeadRevision()
			if err != nil {
				return fmt.Errorf("unable to compute head revision: %w", err)
			}
			_, err = fmt.Println(head)
			return err
		},
	}
	migrationsCmd.AddCommand(headCmd)

	execCmd := &cobra.Command{
		Use:     "exec <migration>",
		Short:   "Exec all migrations up to and including the provided migration",
		Args:    cobra.ExactArgs(1),
		PreRunE: nil, // TODO
		RunE: func(cmd *cobra.Command, args []string) error {
			revision := args[0]
			if revision == "head" {
				head, err := migrations.DatabaseMigrations.HeadRevision()
				if err != nil {
					return fmt.Errorf("unable to compute head revision: %w", err)
				}
				revision = head
			}

			datastoreEngine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
			dbURL := cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri")
			timeout := cobrautil.MustGetDuration(cmd, "timeout")
			batchSize := cobrautil.MustGetUint64(cmd, "batch-size")

			log.Ctx(cmd.Context()).Info().Str("migration", args[0]).Msg("executing migrations")

			var credentialsProvider datastore.CredentialsProvider
			credentialsProviderName := cobrautil.MustGetString(cmd, "datastore-credentials-provider-name")
			if credentialsProviderName != "" {
				var err error
				credentialsProvider, err = datastore.NewCredentialsProvider(cmd.Context(), credentialsProviderName)
				if err != nil {
					return err
				}
			}

			migrationDriver, err := migrations.NewAlembicPostgresDriver(cmd.Context(), dbURL, credentialsProvider)
			if err != nil {
				return fmt.Errorf("unable to create migration driver for %s: %w", datastoreEngine, err)
			}

			return migrate.RunMigration(
				cmd.Context(),
				migrationDriver,
				migrations.DatabaseMigrations,
				revision,
				timeout,
				batchSize,
			)
		},
	}

	pgCmd.AddCommand(migrationsCmd)

	return pgCmd
}

func RegisterPostgresFlags(cmd *cobra.Command) {
}
