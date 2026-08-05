package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
	"github.com/authzed/spicedb/pkg/migrate"
)

// MigrateConfig holds configuration for running database migrations.
type MigrateConfig = migration.Config

// RegisterMigratableEngine makes a datastore engine defined outside this
// package available to the migrate and head commands. Engine-specific
// command-line flags are available to newDriver via MigrateConfig.ExtraConfig.
// verifiableMigrationName is the earliest migration at which the engine's
// current datastore code can open the datastore and read and write data.
// It must be called before command execution, typically from an init function.
func RegisterMigratableEngine[D migrate.Driver[C, T], C any, T any](
	engineName string,
	manager *migrate.Manager[D, C, T],
	newDriver func(ctx context.Context, cfg *MigrateConfig) (D, error),
	verifiableMigrationName string,
) {
	migration.RegisterMigratableEngine(engineName, manager, newDriver, verifiableMigrationName)
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

	extraConfig := make(map[string]string)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		extraConfig[f.Name] = f.Value.String()
	})

	cfg := &MigrateConfig{
		DatastoreEngine:         cobrautil.MustGetStringExpanded(cmd, "datastore-engine"),
		DatastoreURI:            cobrautil.MustGetStringExpanded(cmd, "datastore-conn-uri"),
		CredentialsProviderName: cobrautil.MustGetString(cmd, "datastore-credentials-provider-name"),
		SpannerCredentialsFile:  cobrautil.MustGetStringExpanded(cmd, "datastore-spanner-credentials"),
		SpannerEmulatorHost:     cobrautil.MustGetString(cmd, "datastore-spanner-emulator-host"),
		MySQLTablePrefix:        cobrautil.MustGetString(cmd, "datastore-mysql-table-prefix"),
		Timeout:                 cobrautil.MustGetDuration(cmd, "migration-timeout"),
		BatchSize:               cobrautil.MustGetUint64(cmd, "migration-backfill-batch-size"),
		ExtraConfig:             extraConfig,
	}

	return migration.Run(cmd.Context(), cfg, args[0])
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
			engine := cobrautil.MustGetStringExpanded(cmd, "datastore-engine")
			headRevision, err := migration.HeadRevision(engine)
			if err != nil {
				return fmt.Errorf("unable to compute head revision: %w", err)
			}
			fmt.Println(headRevision)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
}
