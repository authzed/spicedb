package cmd

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/pkg/cmd/demo"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
)

func RegisterDemoFlags(cmd *cobra.Command, config *demo.Config) {
	RegisterTestingFlags(cmd, config.TestServerConfig)
	cmd.Flags().StringVar(&config.DataSet, "dataset", demo.DefaultDataSetName, "demo dataset to preload into each token-scoped datastore")
}

func NewDemoCommand(programName string, config *demo.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "demo",
		Short: "test server with an in-memory datastore and pre-loaded data",
		Long: `An in-memory spicedb server with pre-loaded data which serves completely isolated datastores per client-supplied auth token used.
             
Examples:
  # Run the demo server with the basic dataset
  spicedb demo --dataset basic
  
  # Run the demo server with a custom dataset loaded from a file
  spicedb demo --dataset custom --load-configs /path/to/config.yaml
  
  # Available demo datasets include the following:
  #   sample: user, groups, documents with relationships between them
  #   basic: only relationships, no schema, for testing schema-less operations
  #   docs: a dataset used for documentation examples, with a variety of schema and relationships
  #   custom: no dataset provided by default, intended for use with user-provided datasets via --load-configs
  #   entitlements: a dataset modeling a SaaS application with users, teams, and projects
  #   github: a dataset modeling GitHub with users, teams, organizations, and repositories
  #   user-defined: a dataset modeling a user-defined application with users, roles, and projects
`,
		PreRunE: server.DefaultPreRunE(programName),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			bootstrapContents, err := config.BootstrapConfig()
			if err != nil {
				return err
			}

			if len(bootstrapContents) > 0 {
				if config.TestServerConfig.LoadConfigsContents == nil {
					config.TestServerConfig.LoadConfigsContents = map[string][]byte{}
				}
				for name, contents := range bootstrapContents {
					config.TestServerConfig.LoadConfigsContents[name] = contents
				}
			}

			srv, err := config.TestServerConfig.Complete(cmd.Context())
			if err != nil {
				return err
			}

			signalctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			return srv.Run(signalctx)
		}),
	}
}
