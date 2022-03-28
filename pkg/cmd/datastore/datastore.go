package datastore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alecthomas/units"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/pkg/validationfile"
)

type engineBuilderFunc func(options Config) (datastore.Datastore, error)

var builderForEngine = map[string]engineBuilderFunc{
	"cockroachdb": newCRDBDatastore,
	"postgres":    newPostgresDatastore,
	"memory":      newMemoryDatstore,
	"mysql":       newMysqlDatastore,
}

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	Engine               string
	URI                  string
	GCWindow             time.Duration
	RevisionQuantization time.Duration

	// Options
	MaxIdleTime    time.Duration
	MaxLifetime    time.Duration
	MaxOpenConns   int
	MinOpenConns   int
	SplitQuerySize string
	ReadOnly       bool

	// Bootstrap
	BootstrapFiles     []string
	BootstrapOverwrite bool

	// Hedging
	RequestHedgingEnabled          bool
	RequestHedgingInitialSlowValue time.Duration
	RequestHedgingMaxRequests      uint64
	RequestHedgingQuantile         float64

	// CRDB
	FollowerReadDelay time.Duration
	MaxRetries        int
	OverlapKey        string
	OverlapStrategy   string

	// Postgres
	HealthCheckPeriod  time.Duration
	GCInterval         time.Duration
	GCMaxOperationTime time.Duration

	// MySQL
	TablePrefix string
}

type processedOptions struct {
	SplitQuerySize units.Base2Bytes
}

func (o *Config) ToOption() ConfigOption {
	return func(to *Config) {
		to.Engine = o.Engine
		to.URI = o.URI
		to.GCWindow = o.GCWindow
		to.RevisionQuantization = o.RevisionQuantization
		to.MaxLifetime = o.MaxLifetime
		to.MaxIdleTime = o.MaxIdleTime
		to.MaxOpenConns = o.MaxOpenConns
		to.MinOpenConns = o.MinOpenConns
		to.SplitQuerySize = o.SplitQuerySize
		to.ReadOnly = o.ReadOnly
		to.BootstrapFiles = o.BootstrapFiles
		to.BootstrapOverwrite = o.BootstrapOverwrite
		to.RequestHedgingEnabled = o.RequestHedgingEnabled
		to.RequestHedgingInitialSlowValue = o.RequestHedgingInitialSlowValue
		to.RequestHedgingMaxRequests = o.RequestHedgingMaxRequests
		to.RequestHedgingQuantile = o.RequestHedgingQuantile
		to.FollowerReadDelay = o.FollowerReadDelay
		to.MaxRetries = o.MaxRetries
		to.OverlapKey = o.OverlapKey
		to.OverlapStrategy = o.OverlapStrategy
		to.HealthCheckPeriod = o.HealthCheckPeriod
		to.GCInterval = o.GCInterval
		to.GCMaxOperationTime = o.GCMaxOperationTime
		to.TablePrefix = o.TablePrefix
	}
}

// RegisterDatastoreFlags adds datastore flags to a cobra command
func RegisterDatastoreFlags(cmd *cobra.Command, opts *Config) {
	cmd.Flags().StringVar(&opts.Engine, "datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb", "mysql")`)
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
	cmd.Flags().BoolVar(&opts.ReadOnly, "datastore-readonly", false, "set the service to read-only mode")
	cmd.Flags().StringSliceVar(&opts.BootstrapFiles, "datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	cmd.Flags().BoolVar(&opts.BootstrapOverwrite, "datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")
	cmd.Flags().BoolVar(&opts.RequestHedgingEnabled, "datastore-request-hedging", true, "enable request hedging")
	cmd.Flags().DurationVar(&opts.RequestHedgingInitialSlowValue, "datastore-request-hedging-initial-slow-value", 10*time.Millisecond, "initial value to use for slow datastore requests, before statistics have been collected")
	cmd.Flags().Uint64Var(&opts.RequestHedgingMaxRequests, "datastore-request-hedging-max-requests", 1_000_000, "maximum number of historical requests to consider")
	cmd.Flags().Float64Var(&opts.RequestHedgingQuantile, "datastore-request-hedging-quantile", 0.95, "quantile of historical datastore request time over which a request will be considered slow")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	cmd.Flags().DurationVar(&opts.FollowerReadDelay, "datastore-follower-read-delay-duration", 4_800*time.Millisecond, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach driver only)")
	cmd.Flags().StringVar(&opts.SplitQuerySize, "datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "estimated number of bytes at which a query is split when using a remote datastore")
	cmd.Flags().IntVar(&opts.MaxRetries, "datastore-max-tx-retries", 50, "number of times a retriable transaction should be retried (cockroach driver only)")
	cmd.Flags().StringVar(&opts.OverlapStrategy, "datastore-tx-overlap-strategy", "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	cmd.Flags().StringVar(&opts.OverlapKey, "datastore-tx-overlap-key", "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")
	cmd.Flags().StringVar(&opts.TablePrefix, "datastore-table-prefix", "", "prefix to add to the name of all SpiceDB database tables (mysql driver only)")
}

func DefaultDatastoreConfig() *Config {
	return &Config{
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
func NewDatastore(options ...ConfigOption) (datastore.Datastore, error) {
	var opts Config
	for _, o := range options {
		o(&opts)
	}

	dsBuilder, ok := builderForEngine[opts.Engine]
	if !ok {
		return nil, fmt.Errorf("unknown datastore engine type: %s", opts.Engine)
	}
	log.Info().Msgf("using %s datastore engine", opts.Engine)

	ds, err := dsBuilder(opts)
	if err != nil {
		return nil, err
	}

	if len(opts.BootstrapFiles) > 0 {
		revision, err := ds.HeadRevision(context.Background())
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}

		nsDefs, err := ds.ListNamespaces(context.Background(), revision)
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}
		if opts.BootstrapOverwrite || len(nsDefs) == 0 {
			log.Info().Msg("initializing datastore from bootstrap files")
			_, _, err = validationfile.PopulateFromFiles(ds, opts.BootstrapFiles)
			if err != nil {
				return nil, fmt.Errorf("failed to load bootstrap files: %w", err)
			}
		} else {
			return nil, errors.New("cannot apply bootstrap data: schema or tuples already exist in the datastore. Delete existing data or set the flag --datastore-bootstrap-overwrite=true")
		}
	}

	if opts.RequestHedgingEnabled {
		log.Info().
			Stringer("initialSlowRequest", opts.RequestHedgingInitialSlowValue).
			Uint64("maxRequests", opts.RequestHedgingMaxRequests).
			Float64("hedgingQuantile", opts.RequestHedgingQuantile).
			Msg("request hedging enabled")

		ds = proxy.NewHedgingProxy(
			ds,
			opts.RequestHedgingInitialSlowValue,
			opts.RequestHedgingMaxRequests,
			opts.RequestHedgingQuantile,
		)
	}

	if opts.ReadOnly {
		log.Warn().Msg("setting the datastore to read-only")
		ds = proxy.NewReadonlyDatastore(ds)
	}
	return ds, nil
}

func newCRDBDatastore(opts Config) (datastore.Datastore, error) {
	options, err := processConfigOptions(opts)
	if err != nil {
		return nil, err
	}

	return crdb.NewCRDBDatastore(
		opts.URI,
		crdb.GCWindow(opts.GCWindow),
		crdb.RevisionQuantization(opts.RevisionQuantization),
		crdb.ConnMaxIdleTime(opts.MaxIdleTime),
		crdb.ConnMaxLifetime(opts.MaxLifetime),
		crdb.MaxOpenConns(opts.MaxOpenConns),
		crdb.MinOpenConns(opts.MinOpenConns),
		crdb.SplitAtEstimatedQuerySize(options.SplitQuerySize),
		crdb.FollowerReadDelay(opts.FollowerReadDelay),
		crdb.MaxRetries(opts.MaxRetries),
		crdb.OverlapKey(opts.OverlapKey),
		crdb.OverlapStrategy(opts.OverlapStrategy),
	)
}

func newPostgresDatastore(opts Config) (datastore.Datastore, error) {
	options, err := processConfigOptions(opts)
	if err != nil {
		return nil, err
	}

	return postgres.NewPostgresDatastore(
		opts.URI,
		postgres.GCWindow(opts.GCWindow),
		postgres.RevisionFuzzingTimedelta(opts.RevisionQuantization),
		postgres.ConnMaxIdleTime(opts.MaxIdleTime),
		postgres.ConnMaxLifetime(opts.MaxLifetime),
		postgres.MaxOpenConns(opts.MaxOpenConns),
		postgres.MinOpenConns(opts.MinOpenConns),
		postgres.SplitAtEstimatedQuerySize(options.SplitQuerySize),
		postgres.HealthCheckPeriod(opts.HealthCheckPeriod),
		postgres.GCInterval(opts.GCInterval),
		postgres.GCMaxOperationTime(opts.GCMaxOperationTime),
		postgres.EnablePrometheusStats(),
		postgres.EnableTracing(),
	)
}

func newMemoryDatstore(opts Config) (datastore.Datastore, error) {
	log.Warn().Msg("in-memory datastore is not persistent and not feasible to run in a high availability fashion")
	_, err := processConfigOptions(opts)
	if err != nil {
		return nil, err
	}
	return memdb.NewMemdbDatastore(0, opts.RevisionQuantization, opts.GCWindow, 0)
}

func newMysqlDatastore(opts Config) (datastore.Datastore, error) {
	_, err := processConfigOptions(opts)
	if err != nil {
		return nil, err
	}

	return mysql.NewMysqlDatastore(
		opts.URI,
		mysql.GCInterval(opts.GCInterval),
		mysql.GCWindow(opts.GCWindow),
		mysql.GCInterval(opts.GCInterval),
		mysql.ConnMaxIdleTime(opts.MaxIdleTime),
		mysql.ConnMaxLifetime(opts.MaxLifetime),
		mysql.MaxOpenConns(opts.MaxOpenConns),
		mysql.RevisionFuzzingTimedelta(opts.RevisionQuantization),
		mysql.TablePrefix(opts.TablePrefix),
		mysql.EnablePrometheusStats(),
	)
}

func processConfigOptions(opts Config) (*processedOptions, error) {
	var options processedOptions

	if opts.Engine != "mysql" && opts.TablePrefix != "" {
		return nil, fmt.Errorf("table-prefix option is not compatible with the %s datastore", opts.Engine)
	} else if opts.Engine == "postgres" || opts.Engine == "cockroachdb" {
		var err error
		options.SplitQuerySize, err = units.ParseBase2Bytes(opts.SplitQuerySize)
		if err != nil {
			return nil, fmt.Errorf("failed to parse split query size: %w", err)
		}
	}

	return &options, nil
}
