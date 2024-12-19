package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"

	sqlDriver "github.com/go-sql-driver/mysql"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	mysqlmigrations "github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	spannermigrations "github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func RegisterMigrateFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "memory", fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	cmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().String("datastore-credentials-provider-name", "", fmt.Sprintf(`retrieve datastore credentials dynamically using (%s)`, datastore.CredentialsProviderOptions()))
	cmd.Flags().String("datastore-spanner-credentials", "", "path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)")
	cmd.Flags().String("datastore-spanner-emulator-host", "", "URI of spanner emulator instance used for development and testing (e.g. localhost:9010)")
	cmd.Flags().String("datastore-mysql-table-prefix", "", "prefix to add to the name of all mysql database tables")
	cmd.Flags().Uint64("migration-backfill-batch-size", 1000, "number of items to migrate per iteration of a datastore backfill")
	cmd.Flags().Duration("migration-timeout", 1*time.Hour, "defines a timeout for the execution of the migration, set to 1 hour by default")

	util.RegisterCommonFlags(cmd)
}

func NewMigrateCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "migrate [revision]",
		Short:   "execute datastore schema migrations",
		Long:    fmt.Sprintf("Executes datastore schema migrations for the datastore.\nThe special value \"%s\" can be used to migrate to the latest revision.", color.YellowString(migrate.Head)),
		PreRunE: server.DefaultPreRunE(programName),
		RunE:    termination.PublishError(migrateRun),
		Args:    cobra.ExactArgs(1),
	}
}

func migrateRun(cmd *cobra.Command, args []string) error {
	datastoreEngine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
	dbURL := cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri")
	timeout := cobrautil.MustGetDuration(cmd, "migration-timeout")
	migrationBatachSize := cobrautil.MustGetUint64(cmd, "migration-backfill-batch-size")

	if datastoreEngine == "cockroachdb" {
		log.Ctx(cmd.Context()).Info().Msg("migrating cockroachdb datastore")

		var err error
		migrationDriver, err := crdbmigrations.NewCRDBDriver(dbURL)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", datastoreEngine, err)
		}
		return runMigration(cmd.Context(), migrationDriver, crdbmigrations.CRDBMigrations, args[0], timeout, migrationBatachSize)
	} else if datastoreEngine == "postgres" {
		log.Ctx(cmd.Context()).Info().Msg("migrating postgres datastore")

		var credentialsProvider datastore.CredentialsProvider
		credentialsProviderName := cobrautil.MustGetString(cmd, "datastore-credentials-provider-name")
		if credentialsProviderName != "" {
			var err error
			credentialsProvider, err = datastore.NewCredentialsProvider(cmd.Context(), credentialsProviderName)
			if err != nil {
				return err
			}
		}

		migrationDriver, err := migrations.NewAlembicPostgresDriver(cmd.Context(), dbURL, credentialsProvider, false)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", datastoreEngine, err)
		}
		return runMigration(cmd.Context(), migrationDriver, migrations.DatabaseMigrations, args[0], timeout, migrationBatachSize)
	} else if datastoreEngine == "spanner" {
		log.Ctx(cmd.Context()).Info().Msg("migrating spanner datastore")

		credFile := cobrautil.MustGetStringExpanded(cmd, "datastore-spanner-credentials")
		var err error
		emulatorHost, err := cmd.Flags().GetString("datastore-spanner-emulator-host")
		if err != nil {
			log.Ctx(cmd.Context()).Fatal().Err(err).Msg("unable to get spanner emulator host")
		}
		migrationDriver, err := spannermigrations.NewSpannerDriver(cmd.Context(), dbURL, credFile, emulatorHost)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", datastoreEngine, err)
		}
		return runMigration(cmd.Context(), migrationDriver, spannermigrations.SpannerMigrations, args[0], timeout, migrationBatachSize)
	} else if datastoreEngine == "mysql" {
		log.Ctx(cmd.Context()).Info().Msg("migrating mysql datastore")

		var err error
		tablePrefix, err := cmd.Flags().GetString("datastore-mysql-table-prefix")
		if err != nil {
			log.Ctx(cmd.Context()).Fatal().Msg(fmt.Sprintf("unable to get table prefix: %s", err))
		}

		var credentialsProvider datastore.CredentialsProvider
		credentialsProviderName := cobrautil.MustGetString(cmd, "datastore-credentials-provider-name")
		if credentialsProviderName != "" {
			var err error
			credentialsProvider, err = datastore.NewCredentialsProvider(cmd.Context(), credentialsProviderName)
			if err != nil {
				return err
			}
		}

		// Do this outside NewMySQLDriverFromDSN to avoid races on MySQL datastore tests
		err = sqlDriver.SetLogger(&log.Logger)
		if err != nil {
			return fmt.Errorf("unable to set logging to mysql driver: %w", err)
		}

		migrationDriver, err := mysqlmigrations.NewMySQLDriverFromDSN(dbURL, tablePrefix, credentialsProvider)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", datastoreEngine, err)
		}
		return runMigration(cmd.Context(), migrationDriver, mysqlmigrations.Manager, args[0], timeout, migrationBatachSize)
	}

	return fmt.Errorf("cannot migrate datastore engine type: %s", datastoreEngine)
}

func runMigration[D migrate.Driver[C, T], C any, T any](
	ctx context.Context,
	driver D,
	manager *migrate.Manager[D, C, T],
	targetRevision string,
	timeout time.Duration,
	backfillBatchSize uint64,
) error {
	log.Ctx(ctx).Info().Str("targetRevision", targetRevision).Msg("running migrations")
	ctxWithBatch := context.WithValue(ctx, migrate.BackfillBatchSize, backfillBatchSize)
	ctx, cancel := context.WithTimeout(ctxWithBatch, timeout)
	defer cancel()
	if err := manager.Run(ctx, driver, targetRevision, migrate.LiveRun); err != nil {
		return fmt.Errorf("unable to migrate to `%s` revision: %w", targetRevision, err)
	}

	if err := driver.Close(ctx); err != nil {
		return fmt.Errorf("unable to close migration driver: %w", err)
	}
	return nil
}

func RegisterHeadFlags(cmd *cobra.Command) {
	cmd.Flags().String("datastore-engine", "postgres", fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	util.RegisterCommonFlags(cmd)
}

func NewHeadCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "head",
		Short:   "compute the head database migration revision",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			headRevision, err := HeadRevision(cobrautil.MustGetStringExpanded(cmd, "datastore-engine"))
			if err != nil {
				return fmt.Errorf("unable to compute head revision: %w", err)
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
