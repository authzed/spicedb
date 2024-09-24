package main

import (
	"errors"
	"fmt"
	"os"

	mcobra "github.com/muesli/mango-cobra"
	"github.com/muesli/roff"
	"github.com/rs/zerolog"
	"github.com/sercand/kuberesolver/v5"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/balancer"

	_ "google.golang.org/grpc/xds"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd"
	cmdutil "github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/testserver"
	_ "github.com/authzed/spicedb/pkg/runtime"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var errParsing = errors.New("parsing error")

func main() {
	// Set up root logger
	// This will typically be overwritten by the logging setup for a given command.
	log.SetGlobalLogger(zerolog.New(os.Stderr).Level(zerolog.InfoLevel))

	// Enable Kubernetes gRPC resolver
	kuberesolver.RegisterInCluster()

	// Enable consistent hashring gRPC load balancer
	balancer.Register(cmdutil.ConsistentHashringBuilder)

	// Create a root command
	rootCmd := cmd.NewRootCommand("spicedb")
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmd.Println(err)
		cmd.Println(cmd.UsageString())
		return errParsing
	})
	if err := cmd.RegisterRootFlags(rootCmd); err != nil {
		log.Fatal().Err(err).Msg("failed to register root flags")
	}

	// Add a version command
	versionCmd := cmd.NewVersionCommand(rootCmd.Use)
	cmd.RegisterVersionFlags(versionCmd)
	rootCmd.AddCommand(versionCmd)

	// Add datastore commands
	datastoreCmd, err := cmd.NewDatastoreCommand(rootCmd.Use)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register datastore command")
	}

	cmd.RegisterDatastoreRootFlags(datastoreCmd)
	rootCmd.AddCommand(datastoreCmd)

	// Add deprecated head command
	headCmd := cmd.NewHeadCommand(rootCmd.Use)
	cmd.RegisterHeadFlags(headCmd)
	headCmd.Hidden = true
	headCmd.RunE = cmd.DeprecatedRunE(headCmd.RunE, "spicedb datastore head")
	rootCmd.AddCommand(headCmd)

	// Add deprecated migrate command
	migrateCmd := cmd.NewMigrateCommand(rootCmd.Use)
	migrateCmd.Hidden = true
	migrateCmd.RunE = cmd.DeprecatedRunE(migrateCmd.RunE, "spicedb datastore migrate")
	cmd.RegisterMigrateFlags(migrateCmd)
	rootCmd.AddCommand(migrateCmd)

	// Add server commands
	serverConfig := cmdutil.NewConfigWithOptionsAndDefaults()
	serveCmd := cmd.NewServeCommand(rootCmd.Use, serverConfig)
	if err := cmd.RegisterServeFlags(serveCmd, serverConfig); err != nil {
		log.Fatal().Err(err).Msg("failed to register server flags")
	}
	rootCmd.AddCommand(serveCmd)

	devtoolsCmd := cmd.NewDevtoolsCommand(rootCmd.Use)
	cmd.RegisterDevtoolsFlags(devtoolsCmd)
	rootCmd.AddCommand(devtoolsCmd)

	lspConfig := new(cmd.LSPConfig)
	lspCmd := cmd.NewLSPCommand(rootCmd.Use, lspConfig)
	if err := cmd.RegisterLSPFlags(lspCmd, lspConfig); err != nil {
		log.Fatal().Err(err).Msg("failed to register lsp flags")
	}
	rootCmd.AddCommand(lspCmd)

	var testServerConfig testserver.Config
	testingCmd := cmd.NewTestingCommand(rootCmd.Use, &testServerConfig)
	cmd.RegisterTestingFlags(testingCmd, &testServerConfig)
	rootCmd.AddCommand(testingCmd)

	rootCmd.AddCommand(&cobra.Command{
		Use:                   "man",
		Short:                 "Generate the SpiceDB manpage",
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Hidden:                true,
		Args:                  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			manPage, err := mcobra.NewManPage(1, cmd.Root())
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(os.Stdout, manPage.Build(roff.NewDocument()))
			return err
		},
	})

	if err := rootCmd.Execute(); err != nil {
		if !errors.Is(err, errParsing) {
			log.Err(err).Msg("terminated with errors")
		}
		var termErr spiceerrors.TerminationError
		if errors.As(err, &termErr) {
			os.Exit(termErr.ExitCode())
		}
		os.Exit(1)
	}
}
