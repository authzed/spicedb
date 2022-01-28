package migrate

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	mysqlmigrations "github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	psqlmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	cmdutil "github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/migrate"
)

func RegisterMigrateFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb", "mysql")`)
	cmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
}

func NewMigrateCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "migrate [revision]",
		Short:   "execute datastore schema migrations",
		Long:    fmt.Sprintf("Executes datastore schema migrations for the datastore.\nThe special value \"%s\" can be used to migrate to the latest revision.", color.YellowString(migrate.Head)),
		PreRunE: cmdutil.DefaultPreRunE(programName),
		RunE:    migrateRun,
		Args:    cobra.ExactArgs(1),
	}
}

func migrateRun(cmd *cobra.Command, args []string) error {
	datastoreEngine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
	dbURL := cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri")

	targetRevision := args[0]

	migrationManager, migrationDriver, err := datastoreManagerAndDriver(datastoreEngine, dbURL)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create migration driver")
	}

	log.Info().Msg(fmt.Sprintf("migrating %s datastore", datastoreEngine))
	err = runDatastoreMigrations(migrationManager, migrationDriver, targetRevision)
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
		PreRunE: cmdutil.DefaultPreRunE(programName),
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
		headRevision, err = psqlmigrations.DatabaseMigrations.HeadRevision()
	case "mysql":
		headRevision, err = mysqlmigrations.DatabaseMigrations.HeadRevision()
	default:
		return fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
	if err != nil {
		log.Fatal().Err(err).Msg("unable to compute head revision")
	}

	fmt.Println(headRevision)
	return nil
}

func datastoreManagerAndDriver(datastoreEngine, dbURL string) (*migrate.Manager, migrate.Driver, error) {
	var migrationDriver migrate.Driver
	var migrationManager *migrate.Manager
	var err error

	if datastoreEngine == "cockroachdb" {
		migrationDriver, err = crdbmigrations.NewCRDBDriver(dbURL)
		if err != nil {
			return nil, nil, err
		}

		migrationManager = crdbmigrations.CRDBMigrations
	} else if datastoreEngine == "postgres" {
		migrationDriver, err = psqlmigrations.NewAlembicPostgresDriver(dbURL)
		if err != nil {
			return nil, nil, err
		}

		migrationManager = psqlmigrations.DatabaseMigrations
	} else if datastoreEngine == "mysql" {
		migrationDriver, err = mysqlmigrations.NewMysqlDriver(dbURL)
		if err != nil {
			return nil, nil, err
		}

		migrationManager = mysqlmigrations.DatabaseMigrations
	} else {
		return nil, nil, fmt.Errorf("cannot migrate datastore engine type: %s", datastoreEngine)
	}

	return migrationManager, migrationDriver, nil
}

func runDatastoreMigrations(migrationMananger *migrate.Manager, migrationDriver migrate.Driver, targetRevision string) error {
	log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
	err := migrationMananger.Run(migrationDriver, targetRevision, migrate.LiveRun)
	if err != nil {
		return err
	}

	return nil
}
