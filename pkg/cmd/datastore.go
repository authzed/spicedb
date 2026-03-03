package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	log "github.com/authzed/spicedb/internal/logging"
	dscmd "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
)

func RegisterDatastoreRootFlags(_ *cobra.Command) {
}

func NewDatastoreCommand(programName string) (*cobra.Command, error) {
	datastoreCmd := &cobra.Command{
		Use:   "datastore",
		Short: "datastore operations",
		Long:  "Operations against the configured datastore",
	}

	migrateCmd := NewMigrateCommand(programName)
	RegisterMigrateFlags(migrateCmd)
	datastoreCmd.AddCommand(migrateCmd)

	cfg := dscmd.NewConfigWithOptionsAndDefaults()

	gcCmd := NewGCDatastoreCommand(programName, cfg)
	if err := dscmd.RegisterDatastoreFlagsWithPrefix(gcCmd.Flags(), "", cfg); err != nil {
		return nil, err
	}
	util.RegisterCommonFlags(gcCmd)
	datastoreCmd.AddCommand(gcCmd)

	repairCmd := NewRepairDatastoreCommand(programName, cfg)
	if err := dscmd.RegisterDatastoreFlagsWithPrefix(repairCmd.Flags(), "", cfg); err != nil {
		return nil, err
	}
	util.RegisterCommonFlags(repairCmd)
	datastoreCmd.AddCommand(repairCmd)

	headCmd := NewHeadCommand(programName)
	RegisterHeadFlags(headCmd)
	datastoreCmd.AddCommand(headCmd)

	return datastoreCmd, nil
}

func NewGCDatastoreCommand(programName string, cfg *dscmd.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "gc",
		Short:   "executes garbage collection",
		Long:    "Executes garbage collection against the datastore. Deletes stale relationships, expired relationships, and stale transactions.",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			return executeGC(cmd.Context(), cfg)
		}),
	}
}

func executeGC(ctx context.Context, cfg *dscmd.Config) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.GCMaxOperationTime)
	defer cancel()

	// Disable background GC.
	cfg.GCInterval = -1 * time.Hour

	ds, err := dscmd.NewDatastore(ctx, cfg.ToOption())
	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}
	defer func() {
		err = ds.Close()
		if err != nil {
			log.Error().Err(err).Msg("failed to close datastore")
		}
	}()

	gcds := datastore.UnwrapAs[datastore.GarbageCollectableDatastore](ds)
	if gcds == nil {
		return fmt.Errorf("datastore of type '%s' does not support garbage collection", cfg.Engine)
	}

	log.Ctx(ctx).Info().
		Float64("gc_window_seconds", cfg.GCWindow.Seconds()).
		Float64("gc_max_operation_time_seconds", cfg.GCMaxOperationTime.Seconds()).
		Msg("Running garbage collection...")

	err = datastore.RunGarbageCollection(ctx, gcds, cfg.GCWindow)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Info().Msg("Garbage collection completed")
	return nil
}

func NewRepairDatastoreCommand(programName string, cfg *dscmd.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "repair",
		Short:   "executes datastore repair",
		Long:    "Executes a repair operation for the datastore",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			return executeRepair(cfg, args)
		}),
	}
}

func executeRepair(cfg *dscmd.Config, args []string) error {
	ctx := context.Background()

	// Disable background GC.
	cfg.GCInterval = -1 * time.Hour

	ds, err := dscmd.NewDatastore(ctx, cfg.ToOption())
	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}
	defer func() {
		err = ds.Close()
		if err != nil {
			log.Error().Err(err).Msg("failed to close datastore")
		}
	}()

	repairable := datastore.UnwrapAs[datastore.RepairableDatastore](ds)
	if repairable == nil {
		return fmt.Errorf("datastore of type '%s' does not support the repair operation", cfg.Engine)
	}

	if len(args) == 0 {
		fmt.Println()
		fmt.Println("Available repair operations:")
		for _, op := range repairable.RepairOperations() {
			fmt.Printf("\t%s: %s\n", op.Name, op.Description)
		}
		return nil
	}

	operationName := args[0]

	log.Ctx(ctx).Info().Msg("Running repair...")
	err = repairable.Repair(ctx, operationName, true)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Info().Msg("Datastore repair completed")
	return nil
}
