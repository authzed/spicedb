package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	dspkg "github.com/authzed/spicedb/pkg/datastore"
)

func RegisterDatastoreRootFlags(_ *cobra.Command) {
}

func NewDatastoreCommand(_ string) (*cobra.Command, error) {
	datastoreCmd := &cobra.Command{
		Use:   "datastore",
		Short: "datastore operations",
		Long:  "Operations against the configured datastore",
	}

	migrateCmd := NewMigrateCommand(datastoreCmd.Use)
	RegisterMigrateFlags(migrateCmd)
	datastoreCmd.AddCommand(migrateCmd)

	cfg := datastore.Config{}

	gcCmd := NewGCDatastoreCommand(datastoreCmd.Use, &cfg)
	if err := datastore.RegisterDatastoreFlagsWithPrefix(gcCmd.Flags(), "", &cfg); err != nil {
		return nil, err
	}
	datastoreCmd.AddCommand(gcCmd)

	return datastoreCmd, nil
}

func NewGCDatastoreCommand(programName string, cfg *datastore.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "gc",
		Short:   "executes garbage collection",
		Long:    "Executes garbage collection against the datastore",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			// Disable background GC and hedging.
			cfg.GCInterval = -1 * time.Hour
			cfg.RequestHedgingEnabled = false

			ds, err := datastore.NewDatastore(ctx, cfg.ToOption())
			if err != nil {
				return fmt.Errorf("failed to create datastore: %w", err)
			}

			for {
				wds, ok := ds.(dspkg.UnwrappableDatastore)
				if !ok {
					break
				}
				ds = wds.Unwrap()
			}

			gc, ok := ds.(common.GarbageCollector)
			if !ok {
				return fmt.Errorf("datastore of type %T does not support garbage collection", ds)
			}

			log.Ctx(ctx).Info().Msg("Running garbage collection...")
			err = common.RunGarbageCollection(gc, cfg.GCWindow, cfg.GCMaxOperationTime)
			if err != nil {
				return err
			}
			log.Ctx(ctx).Info().Msg("Garbage collection completed")
			return nil
		}),
	}
}
