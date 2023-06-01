package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/cmd/testserver"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

func RegisterTestingFlags(cmd *cobra.Command, config *testserver.Config) {
	util.RegisterGRPCServerFlags(cmd.Flags(), &config.GRPCServer, "grpc", "gRPC", ":50051", true)
	util.RegisterGRPCServerFlags(cmd.Flags(), &config.ReadOnlyGRPCServer, "readonly-grpc", "read-only gRPC", ":50052", true)

	util.RegisterHTTPServerFlags(cmd.Flags(), &config.HTTPGateway, "http", "http", ":8081", false)
	util.RegisterHTTPServerFlags(cmd.Flags(), &config.ReadOnlyHTTPGateway, "readonly-http", "read-only HTTP", ":8082", false)

	cmd.Flags().StringSliceVar(&config.LoadConfigs, "load-configs", []string{}, "configuration yaml files to load")

	// Flags for API behavior
	cmd.Flags().Uint16Var(&config.MaximumUpdatesPerWrite, "write-relationships-max-updates-per-call", 1000, "maximum number of updates allowed for WriteRelationships calls")
	cmd.Flags().Uint16Var(&config.MaximumPreconditionCount, "update-relationships-max-preconditions-per-call", 1000, "maximum number of preconditions allowed for WriteRelationships and DeleteRelationships calls")
	cmd.Flags().IntVar(&config.MaxCaveatContextSize, "max-caveat-context-size", 4096, "maximum allowed size of request caveat context in bytes. A value of zero or less means no limit")
	cmd.Flags().IntVar(&config.MaxRelationshipContextSize, "max-relationship-context-size", 25000, "maximum allowed size of the context to be stored in a relationship")
}

func NewTestingCommand(programName string, config *testserver.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "serve-testing",
		Short:   "test server with an in-memory datastore",
		Long:    "An in-memory spicedb server which serves completely isolated datastores per client-supplied auth token used.",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			signalctx := SignalContextWithGracePeriod(
				context.Background(),
				0,
			)
			srv, err := config.Complete()
			if err != nil {
				return err
			}
			return srv.Run(signalctx)
		}),
	}
}
