package cmd

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	mysqlmigrations "github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	psqlmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/migrate"
)

func RegisterMigrateFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb", "mysql")`)
	cmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().String("datastore-table-prefix", "", "prefix to add to the name of all SpiceDB database tables (mysql driver only)")
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
	tablePrefix, err := cmd.Flags().GetString("datastore-table-prefix")
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("unable to get table prefix: %s", err))
	}

	targetRevision := args[0]

	migrationManager, migrationDriver, err := datastoreManagerAndDriver(datastoreEngine, dbURL, tablePrefix)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create migration driver")
	}

	log.Info().Msg(fmt.Sprintf("migrating %s datastore", datastoreEngine))
	err = runMigrations(migrationManager, migrationDriver, targetRevision)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to complete requested migrations")
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
		headRevision, err = crdbmigrations.Manager.HeadRevision()
	case "postgres":
		headRevision, err = psqlmigrations.Manager.HeadRevision()
	case "mysql":
		headRevision, err = mysqlmigrations.Manager.HeadRevision()
	default:
		return fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("unable to compute head revision")
	}

	fmt.Println(headRevision)
	return nil
}

func datastoreManagerAndDriver(datastoreEngine, dbURL string, tablePrefix string) (*migrate.Manager, migrate.Driver, error) {
	var migrationDriver migrate.Driver
	var migrationManager *migrate.Manager
	var err error

	if datastoreEngine == "cockroachdb" {
		migrationDriver, err = crdbmigrations.NewCRDBDriver(dbURL)
		if err != nil {
			return nil, nil, err
		}

		migrationManager = crdbmigrations.Manager
	} else if datastoreEngine == "postgres" {
		migrationDriver, err = psqlmigrations.NewAlembicPostgresDriver(dbURL)
		if err != nil {
			return nil, nil, err
		}

		migrationManager = psqlmigrations.Manager
	} else if datastoreEngine == "mysql" {
		migrationDriver, err = mysqlmigrations.NewMysqlDriver(dbURL, tablePrefix)
		if err != nil {
			return nil, nil, err
		}

		migrationManager = mysqlmigrations.Manager
	} else {
		return nil, nil, fmt.Errorf("cannot migrate datastore engine type: %s", datastoreEngine)
	}

	return migrationManager, migrationDriver, nil
}

func runMigrations(migrationMananger *migrate.Manager, migrationDriver migrate.Driver, targetRevision string) error {
	log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
	err := migrationMananger.Run(migrationDriver, targetRevision, migrate.LiveRun)
	if err != nil {
		return err
	}

	return nil
}
