package main

import (
	"math/rand"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/cespare/xxhash"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/sercand/kuberesolver/v3"
	"google.golang.org/grpc/balancer"

	consistentbalancer "github.com/authzed/spicedb/pkg/balancer"
	"github.com/authzed/spicedb/pkg/cmd/devsvc"
	"github.com/authzed/spicedb/pkg/cmd/migrate"
	"github.com/authzed/spicedb/pkg/cmd/root"
	"github.com/authzed/spicedb/pkg/cmd/version"
)

const (
	hashringReplicationFactor = 20
	backendsPerKey            = 1
)

var defaultPreRunE = cobrautil.CommandStack(
	cobrautil.SyncViperPreRunE("spicedb"),
	cobrautil.ZeroLogPreRunE("log", zerolog.InfoLevel),
	cobrautil.OpenTelemetryPreRunE("otel", zerolog.InfoLevel),
)

func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func main() {
	// Set up a seed for randomness
	rand.Seed(time.Now().UnixNano())

	// Enable Kubernetes gRPC resolver
	kuberesolver.RegisterInCluster()

	// Enable consistent hashring gRPC load balancer
	balancer.Register(consistentbalancer.NewConsistentHashringBuilder(xxhash.Sum64, hashringReplicationFactor, backendsPerKey))

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
	devSvcCmd := devsvc.NewCommand(rootCmd.Use)
	devsvc.RegisterFlags(devSvcCmd)
	rootCmd.AddCommand(devSvcCmd)

	registerServeCmd(rootCmd)
	registerTestserverCmd(rootCmd)

	_ = rootCmd.Execute()
}
