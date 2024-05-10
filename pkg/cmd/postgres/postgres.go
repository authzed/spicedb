package postgres

import (
	"fmt"
	"time"

	"github.com/go-logr/zerologr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/logging"
	pkgcmd "github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/releases"
	"github.com/authzed/spicedb/pkg/runtime"
)

const (
	exampleWithoutTLS = "pg serve-grpc --grpc-preshared-key secretKeyHere --pg-uri postgres://postgres:password@localhost:5432"
)

func NewPostgresCommand(programName string) (*cobra.Command, error) {
	pgCmd := &cobra.Command{
		Use:     "postgres",
		Aliases: []string{"pg", "postgresql"},
		Short:   "Perform operations on data stored in PostgreSQL",
		GroupID: "datastores",
		Hidden:  false,
	}
	migrationsCmd := NewMigrationCommand(programName)
	pgCmd.AddCommand(migrationsCmd)

	cfg := &server.Config{}
	cfg.DatastoreConfig.Engine = "postgres"
	cfg.NamespaceCacheConfig = pkgcmd.NamespaceCacheConfig
	cfg.ClusterDispatchCacheConfig = server.CacheConfig{}
	cfg.DispatchCacheConfig = server.CacheConfig{}

	serveCmd := &cobra.Command{
		Use:     "serve-grpc",
		Short:   "Serve the SpiceDB gRPC API services",
		Example: pkgcmd.ServeExample(programName, exampleWithoutTLS),
		PreRunE: cobrautil.CommandStack(
			cobraotel.New("spicedb", cobraotel.WithLogger(zerologr.New(&logging.Logger))).RunE(),
			releases.CheckAndLogRunE(),
			runtime.RunE(),
		),
		RunE: pkgcmd.ServeGRPCRunE(cfg),
	}

	nfs := cobrautil.NewNamedFlagSets(serveCmd)
	if err := pkgcmd.RegisterPostgresDatastoreFlags(serveCmd, nfs.FlagSet("Postgres Datastore"), cfg); err != nil {
		return nil, err
	}

	postRegisterFn, err := pkgcmd.RegisterCommonServeFlags(programName, serveCmd, nfs, cfg, true)
	if err != nil {
		return nil, err
	}

	// Flags must be registered to the command after flags are set.
	nfs.AddFlagSets(serveCmd)
	if err := postRegisterFn(); err != nil {
		return nil, err
	}

	pgCmd.AddCommand(serveCmd)

	return pgCmd, nil
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
		Use:   "exec <target migration>",
		Short: "Execute all migrations up to and including the provided migration",
		Args:  cobra.ExactArgs(1),
		RunE:  ExecMigrationRunE,
	}
	migrationsCmd.AddCommand(execCmd)
	RegisterMigrationExecFlags(execCmd)

	repairCmd := &cobra.Command{
		Use:   "repair-txids",
		Short: "Fast-fowards the Postgres txid counter (required for migrating to new instances)",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			ds, err := postgres.NewPostgresDatastore(ctx, cobrautil.MustGetStringExpanded(cmd, "pg-uri"))
			if err != nil {
				return fmt.Errorf("failed to create datastore: %w", err)
			}
			repairable := datastore.UnwrapAs[datastore.RepairableDatastore](ds)
			if repairable == nil {
				return fmt.Errorf("datastore of type %T does not support the repair operation", ds)
			}

			start := time.Now()
			if err := repairable.Repair(ctx, "transaction-ids", true); err != nil {
				return err
			}
			repairDuration := time.Since(start)

			logging.Ctx(ctx).Info().Dur("duration", repairDuration).Msg("datastore repair completed")
			return nil
		},
	}
	repairCmd.Flags().String("pg-uri", "postgres://postgres:password@localhost:5432/spicedb", "connection string in URI format")
	migrationsCmd.AddCommand(repairCmd)

	return migrationsCmd
}

func RegisterMigrationExecFlags(cmd *cobra.Command) {
	cmd.Flags().String("pg-uri", "postgres://postgres:password@localhost:5432/spicedb", "connection string in URI format")
	cmd.Flags().Uint64("backfill-batch-size", 1000, "batch size used when backfilling data")
	cmd.Flags().Duration("timeout", 1*time.Hour, "maximum execution duration for an individual migration")
}

func ExecMigrationRunE(cmd *cobra.Command, args []string) error {
	revision := args[0]
	if revision == "head" {
		head, err := migrations.DatabaseMigrations.HeadRevision()
		if err != nil {
			return fmt.Errorf("unable to compute head revision: %w", err)
		}
		revision = head
	}

	logging.Ctx(cmd.Context()).Info().Str("target", revision).Msg("executing migrations")

	var credentialsProvider datastore.CredentialsProvider
	credentialsProviderName := cobrautil.MustGetString(cmd, "datastore-credentials-provider-name")
	if credentialsProviderName != "" {
		var err error
		credentialsProvider, err = datastore.NewCredentialsProvider(cmd.Context(), credentialsProviderName)
		if err != nil {
			return err
		}
	}

	migrationDriver, err := migrations.NewAlembicPostgresDriver(cmd.Context(), cobrautil.MustGetStringExpanded(cmd, "pg-uri"), credentialsProvider)
	if err != nil {
		return fmt.Errorf("unable to create postgres migration driver: %w", err)
	}

	return migrate.RunMigration(
		cmd.Context(),
		migrationDriver,
		migrations.DatabaseMigrations,
		revision,
		cobrautil.MustGetDuration(cmd, "timeout"),
		cobrautil.MustGetUint64(cmd, "backfill-batch-size"),
	)
}