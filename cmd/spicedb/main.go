package main

import (
	"errors"
	"os"

	"github.com/rs/zerolog"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/balancer"

	_ "google.golang.org/grpc/xds"

	log "github.com/authzed/spicedb/internal/logging"
	cmd "github.com/authzed/spicedb/pkg/cmd"
	cmdutil "github.com/authzed/spicedb/pkg/cmd/server"
	_ "github.com/authzed/spicedb/pkg/runtime"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func main() {
	// Set up root logger
	// This will typically be overwritten by the logging setup for a given command.
	log.SetGlobalLogger(zerolog.New(os.Stderr).Level(zerolog.InfoLevel))

	// Enable Kubernetes gRPC resolver
	kuberesolver.RegisterInCluster()

	// Enable consistent hashring gRPC load balancer
	balancer.Register(cmdutil.ConsistentHashringBuilder)

	// Create a root command
	rootCmd := cmd.InitialiseRootCmd()

	if err := rootCmd.Execute(); err != nil {
		if !errors.Is(err, cmd.ErrParsing) {
			log.Err(err).Msg("terminated with errors")
		}
		var termErr spiceerrors.TerminationError
		if errors.As(err, &termErr) {
			os.Exit(termErr.ExitCode())
		}
		os.Exit(1)
	}
}
