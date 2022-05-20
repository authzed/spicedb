package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	mysqlmigrations "github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	spannermigrations "github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func RegisterMigrateFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "memory", fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	cmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().String("datastore-spanner-credentials", "", "path to service account key credentials file with access to the cloud spanner instance")
	cmd.Flags().String("datastore-spanner-emulator-host", "", "URI of spanner emulator instance used for development and testing (e.g. localhost:9010)")
	cmd.Flags().String("datastore-mysql-table-prefix", "", "prefix to add to the name of all mysql database tables")
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

	var migrationDriver migrate.Driver
	var manager *migrate.Manager

	if datastoreEngine == "cockroachdb" {
		log.Info().Msg("migrating cockroachdb datastore")

		var err error
		migrationDriver, err = crdbmigrations.NewCRDBDriver(dbURL)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}
		manager = crdbmigrations.CRDBMigrations
	} else if datastoreEngine == "postgres" {
		log.Info().Msg("migrating postgres datastore")

		var err error
		migrationDriver, err = migrations.NewAlembicPostgresDriver(dbURL)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}
		manager = migrations.DatabaseMigrations
	} else if datastoreEngine == "spanner" {
		log.Info().Msg("migrating spanner datastore")

		credFile := cobrautil.MustGetStringExpanded(cmd, "datastore-spanner-credentials")
		var err error
		emulatorHost, err := cmd.Flags().GetString("datastore-spanner-emulator-host")
		if err != nil {
			log.Fatal().Err(err).Msg("unable to get spanner emulator host")
		}
		migrationDriver, err = spannermigrations.NewSpannerDriver(dbURL, credFile, emulatorHost)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}
		manager = spannermigrations.SpannerMigrations
	} else if datastoreEngine == "mysql" {
		log.Info().Msg("migrating mysql datastore")

		var err error
		tablePrefix, err := cmd.Flags().GetString("datastore-mysql-table-prefix")
		if err != nil {
			log.Fatal().Msg(fmt.Sprintf("unable to get table prefix: %s", err))
		}

		migrationDriver, err = mysqlmigrations.NewMySQLDriverFromDSN(dbURL, tablePrefix)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}
		manager = mysqlmigrations.Manager
	} else {
		return fmt.Errorf("cannot migrate datastore engine type: %s", datastoreEngine)
	}

	targetRevision := args[0]

	log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
	if err := manager.Run(migrationDriver, targetRevision, migrate.LiveRun); err != nil {
		log.Fatal().Err(err).Msg("unable to complete requested migrations")
	}

	if err := migrationDriver.Close(); err != nil {
		log.Fatal().Err(err).Msg("unable to close migration driver")
	}

	return nil
}

func RegisterHeadFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "postgres", fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
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
	case "mysql":
		return mysqlmigrations.Manager.HeadRevision()
	case "spanner":
		return spannermigrations.SpannerMigrations.HeadRevision()
	default:
		return "", fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
}
