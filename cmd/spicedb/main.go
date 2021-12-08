package main

import (
	"math/rand"
	"time"

	"github.com/cespare/xxhash"
	"github.com/sercand/kuberesolver/v3"
	"google.golang.org/grpc/balancer"

	consistentbalancer "github.com/authzed/spicedb/pkg/balancer"
	cmdutil "github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/cmd/migrate"
	"github.com/authzed/spicedb/pkg/cmd/root"
	"github.com/authzed/spicedb/pkg/cmd/serve"
	"github.com/authzed/spicedb/pkg/cmd/version"
)

const (
	hashringReplicationFactor = 20
	backendsPerKey            = 1
)

func main() {
	// Set up a seed for randomness
	rand.Seed(time.Now().UnixNano())

	// Enable Kubernetes gRPC resolver
	kuberesolver.RegisterInCluster()

	// Enable consistent hashring gRPC load balancer
	balancer.Register(consistentbalancer.NewConsistentHashringBuilder(
		xxhash.Sum64,
		hashringReplicationFactor,
		backendsPerKey,
	))

	// Create a root command
	rootCmd := root.NewCommand()
	root.RegisterFlags(rootCmd)

	// Add a version command
	versionCmd := version.NewCommand(rootCmd.Use)
	version.RegisterVersionFlags(versionCmd)
	rootCmd.AddCommand(versionCmd)

	// Add migration commands
	migrateCmd := migrate.NewMigrateCommand(rootCmd.Use)
	migrate.RegisterMigrateFlags(migrateCmd)
	rootCmd.AddCommand(migrateCmd)

	headCmd := migrate.NewHeadCommand(rootCmd.Use)
	migrate.RegisterHeadFlags(headCmd)
	rootCmd.AddCommand(headCmd)

	// Add server commands
	var dsConfig cmdutil.DatastoreConfig
	serveCmd := serve.NewServeCommand(rootCmd.Use, &dsConfig)
	serve.RegisterServeFlags(serveCmd, &dsConfig)
	rootCmd.AddCommand(serveCmd)

	devtoolsCmd := serve.NewDevtoolsCommand(rootCmd.Use)
	serve.RegisterDevtoolsFlags(devtoolsCmd)
	rootCmd.AddCommand(devtoolsCmd)

	testingCmd := serve.NewTestingCommand(rootCmd.Use)
	serve.RegisterTestingFlags(testingCmd)
	rootCmd.AddCommand(testingCmd)

	_ = rootCmd.Execute()
}
