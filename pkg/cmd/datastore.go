package cmd

import (
	"fmt"
	"time"

	"github.com/alecthomas/units"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
)

type engineBuilderFunc func(options DatastoreConfig) (datastore.Datastore, error)

var builderForEngine = map[string]engineBuilderFunc{
	"cockroachdb": newCRDBDatastore,
	"postgres":    newPostgresDatastore,
	"memory":      newMemoryDatstore,
}

type DatastoreOption func(*DatastoreConfig)

//go:generate go run github.com/ecordell/optgen -output zz_generated.datastore_options.go . DatastoreConfig
type DatastoreConfig struct {
	Engine               string
	URI                  string
	GCWindow             time.Duration
	RevisionQuantization time.Duration

	MaxIdleTime    time.Duration
	MaxLifetime    time.Duration
	MaxOpenConns   int
	MinOpenConns   int
	SplitQuerySize string

	// CRDB
	FollowerReadDelay time.Duration
	MaxRetries        int
	OverlapKey        string
	OverlapStrategy   string

	// Postgres
	HealthCheckPeriod  time.Duration
	GCInterval         time.Duration
	GCMaxOperationTime time.Duration
}

func (o *DatastoreConfig) ToOption() DatastoreOption {
	return func(to *DatastoreConfig) {
		to.Engine = o.Engine
		to.URI = o.URI
		to.GCWindow = o.GCWindow
		to.RevisionQuantization = o.RevisionQuantization
		to.MaxLifetime = o.MaxLifetime
		to.MaxIdleTime = o.MaxIdleTime
		to.MaxOpenConns = o.MaxOpenConns
		to.MinOpenConns = o.MinOpenConns
		to.SplitQuerySize = o.SplitQuerySize
		to.FollowerReadDelay = o.FollowerReadDelay
		to.MaxRetries = o.MaxRetries
		to.OverlapKey = o.OverlapKey
		to.OverlapStrategy = o.OverlapStrategy
		to.HealthCheckPeriod = o.HealthCheckPeriod
		to.GCInterval = o.GCInterval
		to.GCMaxOperationTime = o.GCMaxOperationTime
	}
}

// RegisterDatastoreFlags adds datastore flags to a cobra command
func RegisterDatastoreFlags(cmd *cobra.Command, opts *DatastoreConfig) {
	cmd.Flags().StringVar(&opts.Engine, "datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	cmd.Flags().StringVar(&opts.URI, "datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().IntVar(&opts.MaxOpenConns, "datastore-conn-max-open", 20, "number of concurrent connections open in a remote datastore's connection pool")
	cmd.Flags().IntVar(&opts.MinOpenConns, "datastore-conn-min-open", 10, "number of minimum concurrent connections open in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.MaxLifetime, "datastore-conn-max-lifetime", 30*time.Minute, "maximum amount of time a connection can live in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.MaxIdleTime, "datastore-conn-max-idletime", 30*time.Minute, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.HealthCheckPeriod, "datastore-conn-healthcheck-interval", 30*time.Second, "time between a remote datastore's connection pool health checks")
	cmd.Flags().DurationVar(&opts.GCWindow, "datastore-gc-window", 24*time.Hour, "amount of time before revisions are garbage collected")
	cmd.Flags().DurationVar(&opts.GCInterval, "datastore-gc-interval", 3*time.Minute, "amount of time between passes of garbage collection (postgres driver only)")
	cmd.Flags().DurationVar(&opts.GCMaxOperationTime, "datastore-gc-max-operation-time", 1*time.Minute, "maximum amount of time a garbage collection pass can operate before timing out (postgres driver only)")
	cmd.Flags().DurationVar(&opts.RevisionQuantization, "datastore-revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	cmd.Flags().DurationVar(&opts.FollowerReadDelay, "datastore-follower-read-delay-duration", 4_800*time.Millisecond, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach driver only)")
	cmd.Flags().StringVar(&opts.SplitQuerySize, "datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "estimated number of bytes at which a query is split when using a remote datastore")
	cmd.Flags().IntVar(&opts.MaxRetries, "datastore-max-tx-retries", 50, "number of times a retriable transaction should be retried (cockroach driver only)")
	cmd.Flags().StringVar(&opts.OverlapStrategy, "datastore-tx-overlap-strategy", "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	cmd.Flags().StringVar(&opts.OverlapKey, "datastore-tx-overlap-key", "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")
}

func DefaultDatastoreConfig() *DatastoreConfig {
	return &DatastoreConfig{
		GCWindow:             24 * time.Hour,
		RevisionQuantization: 5 * time.Second,
		MaxLifetime:          30 * time.Minute,
		MaxIdleTime:          30 * time.Minute,
		MaxOpenConns:         20,
		MinOpenConns:         10,
		SplitQuerySize:       common.DefaultSplitAtEstimatedQuerySize.String(),
		MaxRetries:           50,
		OverlapStrategy:      "prefix",
		HealthCheckPeriod:    30 * time.Second,
		GCInterval:           3 * time.Minute,
		GCMaxOperationTime:   1 * time.Minute,
	}
}

// NewDatastore initializes a datastore given the options
func NewDatastore(options ...DatastoreOption) (datastore.Datastore, error) {
	var opts DatastoreConfig
	for _, o := range options {
		o(&opts)
	}

	dsBuilder, ok := builderForEngine[opts.Engine]
	if !ok {
		return nil, fmt.Errorf("unknown datastore engine type: %s", opts.Engine)
	}
	log.Info().Msgf("using %s datastore engine", opts.Engine)

	return dsBuilder(opts)
}

func newCRDBDatastore(opts DatastoreConfig) (datastore.Datastore, error) {
	splitQuerySize, err := units.ParseBase2Bytes(opts.SplitQuerySize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse split query size: %w", err)
	}
	return crdb.NewCRDBDatastore(
		opts.URI,
		crdb.GCWindow(opts.GCWindow),
		crdb.RevisionQuantization(opts.RevisionQuantization),
		crdb.ConnMaxIdleTime(opts.MaxIdleTime),
		crdb.ConnMaxLifetime(opts.MaxLifetime),
		crdb.MaxOpenConns(opts.MaxOpenConns),
		crdb.MinOpenConns(opts.MinOpenConns),
		crdb.SplitAtEstimatedQuerySize(splitQuerySize),
		crdb.FollowerReadDelay(opts.FollowerReadDelay),
		crdb.MaxRetries(opts.MaxRetries),
		crdb.OverlapKey(opts.OverlapKey),
		crdb.OverlapStrategy(opts.OverlapStrategy),
	)
}

func newPostgresDatastore(opts DatastoreConfig) (datastore.Datastore, error) {
	splitQuerySize, err := units.ParseBase2Bytes(opts.SplitQuerySize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse split query size: %w", err)
	}
	return postgres.NewPostgresDatastore(
		opts.URI,
		postgres.GCWindow(opts.GCWindow),
		postgres.RevisionFuzzingTimedelta(opts.RevisionQuantization),
		postgres.ConnMaxIdleTime(opts.MaxIdleTime),
		postgres.ConnMaxLifetime(opts.MaxLifetime),
		postgres.MaxOpenConns(opts.MaxOpenConns),
		postgres.MinOpenConns(opts.MinOpenConns),
		postgres.SplitAtEstimatedQuerySize(splitQuerySize),
		postgres.HealthCheckPeriod(opts.HealthCheckPeriod),
		postgres.GCInterval(opts.GCInterval),
		postgres.GCMaxOperationTime(opts.GCMaxOperationTime),
		postgres.EnablePrometheusStats(),
		postgres.EnableTracing(),
	)
}

func newMemoryDatstore(opts DatastoreConfig) (datastore.Datastore, error) {
	log.Warn().Msg("in-memory datastore is not persistent and not feasible to run in a high availability fashion")
	return memdb.NewMemdbDatastore(0, opts.RevisionQuantization, opts.GCWindow, 0)
}
