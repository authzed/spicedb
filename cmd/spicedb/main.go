package main

import (
	"errors"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/balancer"
	_ "google.golang.org/grpc/xds"

	"github.com/authzed/spicedb/cmd/spicedb/memoryprotection"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd"
	cmdutil "github.com/authzed/spicedb/pkg/cmd/server"
	_ "github.com/authzed/spicedb/pkg/runtime"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func main() {
	memoryprotection.InitDefaultMemoryUsageProvider()

	// Set up root logger
	// This will typically be overwritten by the logging setup for a given command.
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.SetGlobalLogger(zerolog.New(os.Stderr).Level(zerolog.InfoLevel))

	// Enable Kubernetes gRPC resolver
	kuberesolver.RegisterInCluster()

	// Enable consistent hashring gRPC load balancer
	balancer.Register(cmdutil.ConsistentHashringBuilder)

	// Build the complete command structure
	rootCmd, err := cmd.BuildRootCommand()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to build root command")
	}

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
