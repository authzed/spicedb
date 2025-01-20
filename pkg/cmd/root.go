package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/spf13/cobra"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/testserver"
	"github.com/authzed/spicedb/pkg/releases"
	mcobra "github.com/muesli/mango-cobra"
	"github.com/muesli/roff"
)

var ErrParsing = errors.New("parsing error")

// This function is utilised to generate docs for SpiceDB
func InitialiseRootCmd() *cobra.Command {
	rootCmd := NewRootCommand("spicedb")
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmd.Println(err)
		cmd.Println(cmd.UsageString())
		return ErrParsing
	})
	if err := RegisterRootFlags(rootCmd); err != nil {
		log.Fatal().Err(err).Msg("failed to register root flags")
	}

	// Add a version command
	versionCmd := NewVersionCommand(rootCmd.Use)
	RegisterVersionFlags(versionCmd)
	rootCmd.AddCommand(versionCmd)

	// Add datastore commands
	datastoreCmd, err := NewDatastoreCommand(rootCmd.Use)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to register datastore command")
	}

	RegisterDatastoreRootFlags(datastoreCmd)
	rootCmd.AddCommand(datastoreCmd)

	// Add deprecated head command
	headCmd := NewHeadCommand(rootCmd.Use)
	RegisterHeadFlags(headCmd)
	headCmd.Hidden = true
	headCmd.RunE = DeprecatedRunE(headCmd.RunE, "spicedb datastore head")
	rootCmd.AddCommand(headCmd)

	// Add deprecated migrate command
	migrateCmd := NewMigrateCommand(rootCmd.Use)
	migrateCmd.Hidden = true
	migrateCmd.RunE = DeprecatedRunE(migrateCmd.RunE, "spicedb datastore migrate")
	RegisterMigrateFlags(migrateCmd)
	rootCmd.AddCommand(migrateCmd)

	// Add server commands
	serverConfig := server.NewConfigWithOptionsAndDefaults()
	serveCmd := NewServeCommand(rootCmd.Use, serverConfig)
	if err := RegisterServeFlags(serveCmd, serverConfig); err != nil {
		log.Fatal().Err(err).Msg("failed to register server flags")
	}
	rootCmd.AddCommand(serveCmd)

	devtoolsCmd := NewDevtoolsCommand(rootCmd.Use)
	RegisterDevtoolsFlags(devtoolsCmd)
	rootCmd.AddCommand(devtoolsCmd)

	lspConfig := new(LSPConfig)
	lspCmd := NewLSPCommand(rootCmd.Use, lspConfig)
	if err := RegisterLSPFlags(lspCmd, lspConfig); err != nil {
		log.Fatal().Err(err).Msg("failed to register lsp flags")
	}
	rootCmd.AddCommand(lspCmd)

	var testServerConfig testserver.Config
	testingCmd := NewTestingCommand(rootCmd.Use, &testServerConfig)
	RegisterTestingFlags(testingCmd, &testServerConfig)
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

	return rootCmd
}

func RegisterRootFlags(cmd *cobra.Command) error {
	zl := cobrazerolog.New()
	zl.RegisterFlags(cmd.PersistentFlags())
	if err := zl.RegisterFlagCompletion(cmd); err != nil {
		return fmt.Errorf("failed to register zerolog flag completion: %w", err)
	}

	releases.RegisterFlags(cmd.PersistentFlags())

	return nil
}

// DeprecatedRunE wraps the RunFunc with a warning log statement.
func DeprecatedRunE(fn cobrautil.CobraRunFunc, newCmd string) cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		log.Warn().Str("newCommand", newCmd).Msg("use of deprecated command")
		return fn(cmd, args)
	}
}

func NewRootCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:           programName,
		Short:         "A modern permissions database",
		Long:          "A database that stores, computes, and validates application permissions",
		Example:       server.ServeExample(programName),
		SilenceErrors: true,
		SilenceUsage:  true,
	}
}
