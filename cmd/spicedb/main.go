package main

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog/log"
	"github.com/sercand/kuberesolver/v3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/balancer"
	_ "google.golang.org/grpc/xds"

	consistentbalancer "github.com/authzed/spicedb/pkg/balancer"
	"github.com/authzed/spicedb/pkg/cmd"
	cmdutil "github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/testserver"
)

const (
	hashringReplicationFactor = 20
	backendsPerKey            = 1
)

var errParsing = errors.New("parsing error")

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
	rootCmd := cmd.NewRootCommand("spicedb")
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmd.Println(err)
		cmd.Println(cmd.UsageString())
		fmt.Println("Somewhere in the middle")
		return errParsing
	})
	cmd.RegisterRootFlags(rootCmd)

	// Add a version command
	versionCmd := cmd.NewVersionCommand(rootCmd.Use)
	cmd.RegisterVersionFlags(versionCmd)
	rootCmd.AddCommand(versionCmd)

	// Add migration commands
	migrateCmd := cmd.NewMigrateCommand(rootCmd.Use)
	cmd.RegisterMigrateFlags(migrateCmd)
	rootCmd.AddCommand(migrateCmd)

	headCmd := cmd.NewHeadCommand(rootCmd.Use)
	cmd.RegisterHeadFlags(headCmd)
	rootCmd.AddCommand(headCmd)

	// Add server commands
	var serverConfig cmdutil.Config
	serveCmd := cmd.NewServeCommand(rootCmd.Use, &serverConfig)
	cmd.RegisterServeFlags(serveCmd, &serverConfig)
	rootCmd.AddCommand(serveCmd)

	devtoolsCmd := cmd.NewDevtoolsCommand(rootCmd.Use)
	cmd.RegisterDevtoolsFlags(devtoolsCmd)
	rootCmd.AddCommand(devtoolsCmd)

	var testServerConfig testserver.Config
	testingCmd := cmd.NewTestingCommand(rootCmd.Use, &testServerConfig)
	cmd.RegisterTestingFlags(testingCmd, &testServerConfig)
	rootCmd.AddCommand(testingCmd)
	if err := rootCmd.Execute(); err != nil {
		if !errors.Is(err, errParsing) {
			log.Err(err).Msg("terminated with errors")
		}
		//os.Exit(1)
	}
	fmt.Println("I got here though")
}

func connectUnixSocket() (*sql.DB, error) {
	// Note: Saving credentials in environment variables is convenient, but not
	// secure - consider a more secure solution such as
	// Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
	// keep secrets safe.
	var (
		dbUser         = "user"                                                    // e.g. 'my-db-user'
		dbPwd          = "Happy567"                                                // e.g. 'my-db-password'
		unixSocketPath = "/cloudsql/cog-analytics-backend:us-central1:authz-store" // e.g. '/cloudsql/project:region:instance'
		dbName         = "postgres"                                                // e.g. 'my-database'
	)

	dbURI := fmt.Sprintf("user=%s password=%s database=%s host=%s",
		dbUser, dbPwd, dbName, unixSocketPath)

	// dbPool is the pool of database connections.
	dbPool, err := sql.Open("pgx", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %v", err)
	}

	// ...
	fmt.Println("we have successfully created A connection")
	return dbPool, nil
}
