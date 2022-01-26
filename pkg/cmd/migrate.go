package cmd

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/cmd/server"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
)

func RegisterMigrateFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	cmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
}

func NewMigrateCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "migrate [revision]",
		Short:   "execute datastore schema migrations",
		Long:    fmt.Sprintf("Executes datastore schema migrations for the datastore.\nThe special value \"%s\" can be used to migrate to the latest revision.", color.YellowString(migrate.Head)),
		PreRunE: server.DefaultPreRunE(programName),
		RunE:    migrateRun,
		Args:    cobra.ExactArgs(1),
	}
}

func migrateRun(cmd *cobra.Command, args []string) error {
	datastoreEngine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
	dbURL := cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri")

	if datastoreEngine == "cockroachdb" {
		log.Info().Msg("migrating cockroachdb datastore")

		migrationDriver, err := crdbmigrations.NewCRDBDriver(dbURL)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}

		targetRevision := args[0]

		log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
		err = crdbmigrations.CRDBMigrations.Run(migrationDriver, targetRevision, migrate.LiveRun)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to complete requested migrations")
		}
	} else if datastoreEngine == "postgres" {
		log.Info().Msg("migrating postgres datastore")
		migrationDriver, err := migrations.NewAlembicPostgresDriver(dbURL)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}

		targetRevision := args[0]

		log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
		err = migrations.DatabaseMigrations.Run(migrationDriver, targetRevision, migrate.LiveRun)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to complete requested migrations")
		}
	} else {
		return fmt.Errorf("cannot migrate datastore engine type: %s", datastoreEngine)
	}

	return nil
}

func RegisterHeadFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "postgres", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")
}

func NewHeadCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "head",
		Short:   "compute the head database migration revision",
		PreRunE: server.DefaultPreRunE(programName),
		RunE:    headRevisionRun,
		Args:    cobra.ExactArgs(0),
	}
}

func headRevisionRun(cmd *cobra.Command, args []string) error {
	var (
		engine       = cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
		headRevision string
		err          error
	)

	switch engine {
	case "cockroachdb":
		headRevision, err = crdbmigrations.CRDBMigrations.HeadRevision()
	case "postgres":
		headRevision, err = migrations.DatabaseMigrations.HeadRevision()
	default:
		return fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("unable to compute head revision")
	}

	fmt.Println(headRevision)
	return nil
}
