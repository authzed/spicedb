package cmd

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/testserver"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

func RegisterTestingFlags(cmd *cobra.Command, config *testserver.Config) {
	util.RegisterGRPCServerFlags(cmd.Flags(), &config.GRPCServer, "grpc", "gRPC", ":50051", true)
	util.RegisterGRPCServerFlags(cmd.Flags(), &config.ReadOnlyGRPCServer, "readonly-grpc", "read-only gRPC", ":50052", true)
	cmd.Flags().StringSliceVar(&config.LoadConfigs, "load-configs", []string{}, "configuration yaml files to load")
}

func NewTestingCommand(programName string, config *testserver.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "serve-testing",
		Short:   "test server with an in-memory datastore",
		Long:    "An in-memory spicedb server which serves completely isolated datastores per client-supplied auth token used.",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			signalctx := SignalContextWithGracePeriod(
				context.Background(),
				0,
			)
			srv, err := config.Complete()
			if err != nil {
				return err
			}
			return srv.Run(signalctx)
		},
	}
}
