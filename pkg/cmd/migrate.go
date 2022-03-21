package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	spannermigrations "github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/cmd/migration"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/migrate"
)

func RegisterMigrateFlags(cmd *cobra.Command, config *migration.Config) {
	cmd.Flags().StringVar(&config.DatastoreEngine, "datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	cmd.Flags().StringVar(&config.DatastoreURI, "datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().StringVar(&config.SpannerCredentials, "datastore-spanner-credentials", "", "path to service account key credentials file with access to the cloud spanner instance")
}

func NewMigrateCommand(programName string, config *migration.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "migrate [revision]",
		Short:   "execute datastore schema migrations",
		Long:    fmt.Sprintf("Executes datastore schema migrations for the datastore.\nThe special value \"%s\" can be used to migrate to the latest revision.", color.YellowString(migrate.Head)),
		PreRunE: server.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			migrator, err := config.Complete()
			if err != nil {
				return err
			}
			return migrator.Migrate(args[0])
		},
		Args: cobra.ExactArgs(1),
	}
}

func RegisterHeadFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "postgres", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")
}

func NewHeadCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "head",
		Short:   "compute the head database migration revision",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			headRevision, err := HeadRevision(cobrautil.MustGetStringExpanded(cmd, "datastore-engine"))
			if err != nil {
				log.Fatal().Err(err).Msg("unable to compute head revision")
			}
			fmt.Println(headRevision)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
}

// HeadRevision returns the latest migration revision for a given engine
func HeadRevision(engine string) (string, error) {
	switch engine {
	case "cockroachdb":
		return crdbmigrations.CRDBMigrations.HeadRevision()
	case "postgres":
		return migrations.DatabaseMigrations.HeadRevision()
	case "spanner":
		return spannermigrations.SpannerMigrations.HeadRevision()
	default:
		return "", fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
}
