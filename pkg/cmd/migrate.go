package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fatih/color"
	sqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"

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

// MigrateConfig holds configuration for running database migrations.
type MigrateConfig struct {
	DatastoreEngine         string
	DatastoreURI            string
	CredentialsProviderName string
	SpannerCredentialsFile  string
	SpannerEmulatorHost     string
	MySQLTablePrefix        string
	Timeout                 time.Duration
	BatchSize               uint64
}

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
	}
}

func migrateRun(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("missing required argument: 'revision'")
	}

	cfg := &MigrateConfig{
		DatastoreEngine:         cobrautil.MustGetStringExpanded(cmd, "datastore-engine"),
		DatastoreURI:            cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri"),
		CredentialsProviderName: cobrautil.MustGetString(cmd, "datastore-credentials-provider-name"),
		SpannerCredentialsFile:  cobrautil.MustGetStringExpanded(cmd, "datastore-spanner-credentials"),
		SpannerEmulatorHost:     cobrautil.MustGetString(cmd, "datastore-spanner-emulator-host"),
		MySQLTablePrefix:        cobrautil.MustGetString(cmd, "datastore-mysql-table-prefix"),
		Timeout:                 cobrautil.MustGetDuration(cmd, "migration-timeout"),
		BatchSize:               cobrautil.MustGetUint64(cmd, "migration-backfill-batch-size"),
	}

	return executeMigrate(cmd.Context(), cfg, args[0])
}

// executeMigrate runs the migration with the given configuration.
// This function is extracted to enable testing without cobra command dependencies.
func executeMigrate(ctx context.Context, cfg *MigrateConfig, revision string) error {
	if revision == "" {
		return errors.New("missing required revision")
	}

	switch cfg.DatastoreEngine {
	case "cockroachdb":
		log.Ctx(ctx).Info().Msg("migrating cockroachdb datastore")

		migrationDriver, err := crdbmigrations.NewCRDBDriver(cfg.DatastoreURI)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", cfg.DatastoreEngine, err)
		}
		return runMigration(ctx, migrationDriver, crdbmigrations.CRDBMigrations, revision, cfg.Timeout, cfg.BatchSize)

	case "postgres":
		log.Ctx(ctx).Info().Msg("migrating postgres datastore")

		var credentialsProvider datastore.CredentialsProvider
		if cfg.CredentialsProviderName != "" {
			var err error
			credentialsProvider, err = datastore.NewCredentialsProvider(ctx, cfg.CredentialsProviderName)
			if err != nil {
				return err
			}
		}

		migrationDriver, err := migrations.NewAlembicPostgresDriver(ctx, cfg.DatastoreURI, credentialsProvider, false)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", cfg.DatastoreEngine, err)
		}
		return runMigration(ctx, migrationDriver, migrations.DatabaseMigrations, revision, cfg.Timeout, cfg.BatchSize)

	case "spanner":
		log.Ctx(ctx).Info().Msg("migrating spanner datastore")

		migrationDriver, err := spannermigrations.NewSpannerDriver(ctx, cfg.DatastoreURI, cfg.SpannerCredentialsFile, cfg.SpannerEmulatorHost)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", cfg.DatastoreEngine, err)
		}
		return runMigration(ctx, migrationDriver, spannermigrations.SpannerMigrations, revision, cfg.Timeout, cfg.BatchSize)

	case "mysql":
		log.Ctx(ctx).Info().Msg("migrating mysql datastore")

		var credentialsProvider datastore.CredentialsProvider
		if cfg.CredentialsProviderName != "" {
			var err error
			credentialsProvider, err = datastore.NewCredentialsProvider(ctx, cfg.CredentialsProviderName)
			if err != nil {
				return err
			}
		}

		// Do this outside NewMySQLDriverFromDSN to avoid races on MySQL datastore tests
		if err := sqlDriver.SetLogger(&log.Logger); err != nil {
			return fmt.Errorf("unable to set logging to mysql driver: %w", err)
		}

		migrationDriver, err := mysqlmigrations.NewMySQLDriverFromDSN(cfg.DatastoreURI, cfg.MySQLTablePrefix, credentialsProvider)
		if err != nil {
			return fmt.Errorf("unable to create migration driver for %s: %w", cfg.DatastoreEngine, err)
		}
		return runMigration(ctx, migrationDriver, mysqlmigrations.Manager, revision, cfg.Timeout, cfg.BatchSize)
	}

	return fmt.Errorf("cannot migrate datastore engine type: %s", cfg.DatastoreEngine)
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
		Short:   "compute the head (latest) database migration revision available",
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
