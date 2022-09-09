package datastore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/datastore/spanner"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
)

type engineBuilderFunc func(options Config) (datastore.Datastore, error)

const (
	MemoryEngine    = "memory"
	PostgresEngine  = "postgres"
	CockroachEngine = "cockroachdb"
	SpannerEngine   = "spanner"
	MySQLEngine     = "mysql"
)

var BuilderForEngine = map[string]engineBuilderFunc{
	CockroachEngine: newCRDBDatastore,
	PostgresEngine:  newPostgresDatastore,
	MemoryEngine:    newMemoryDatstore,
	SpannerEngine:   newSpannerDatastore,
	MySQLEngine:     newMySQLDatastore,
}

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	Engine               string
	URI                  string
	GCWindow             time.Duration
	LegacyFuzzing        time.Duration
	RevisionQuantization time.Duration

	// Options
	MaxIdleTime            time.Duration
	MaxLifetime            time.Duration
	MaxOpenConns           int
	MinOpenConns           int
	SplitQueryCount        uint16
	ReadOnly               bool
	EnableDatastoreMetrics bool
	DisableStats           bool

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

	// Spanner
	SpannerCredentialsFile string
	SpannerEmulatorHost    string

	// MySQL
	TablePrefix string

	// Internal
	WatchBufferLength uint16
}

// RegisterDatastoreFlags adds datastore flags to a cobra command
func RegisterDatastoreFlags(cmd *cobra.Command, opts *Config) {
	cmd.Flags().StringVar(&opts.Engine, "datastore-engine", "memory", fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	cmd.Flags().StringVar(&opts.URI, "datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().IntVar(&opts.MaxOpenConns, "datastore-conn-max-open", 20, "number of concurrent connections open in a remote datastore's connection pool")
	cmd.Flags().IntVar(&opts.MinOpenConns, "datastore-conn-min-open", 10, "number of minimum concurrent connections open in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.MaxLifetime, "datastore-conn-max-lifetime", 30*time.Minute, "maximum amount of time a connection can live in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.MaxIdleTime, "datastore-conn-max-idletime", 30*time.Minute, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.HealthCheckPeriod, "datastore-conn-healthcheck-interval", 30*time.Second, "time between a remote datastore's connection pool health checks")
	cmd.Flags().DurationVar(&opts.GCWindow, "datastore-gc-window", 24*time.Hour, "amount of time before revisions are garbage collected")
	cmd.Flags().DurationVar(&opts.GCInterval, "datastore-gc-interval", 3*time.Minute, "amount of time between passes of garbage collection (postgres driver only)")
	cmd.Flags().DurationVar(&opts.GCMaxOperationTime, "datastore-gc-max-operation-time", 1*time.Minute, "maximum amount of time a garbage collection pass can operate before timing out (postgres driver only)")
	cmd.Flags().DurationVar(&opts.RevisionQuantization, "datastore-revision-quantization-interval", 5*time.Second, "boundary interval to which to round the quantized revision")
	cmd.Flags().BoolVar(&opts.ReadOnly, "datastore-readonly", false, "set the service to read-only mode")
	cmd.Flags().StringSliceVar(&opts.BootstrapFiles, "datastore-bootstrap-files", []string{}, "bootstrap data yaml files to load")
	cmd.Flags().BoolVar(&opts.BootstrapOverwrite, "datastore-bootstrap-overwrite", false, "overwrite any existing data with bootstrap data")
	cmd.Flags().BoolVar(&opts.RequestHedgingEnabled, "datastore-request-hedging", true, "enable request hedging")
	cmd.Flags().DurationVar(&opts.RequestHedgingInitialSlowValue, "datastore-request-hedging-initial-slow-value", 10*time.Millisecond, "initial value to use for slow datastore requests, before statistics have been collected")
	cmd.Flags().Uint64Var(&opts.RequestHedgingMaxRequests, "datastore-request-hedging-max-requests", 1_000_000, "maximum number of historical requests to consider")
	cmd.Flags().Float64Var(&opts.RequestHedgingQuantile, "datastore-request-hedging-quantile", 0.95, "quantile of historical datastore request time over which a request will be considered slow")
	cmd.Flags().BoolVar(&opts.EnableDatastoreMetrics, "datastore-prometheus-metrics", true, "set to false to disabled prometheus metrics from the datastore")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	cmd.Flags().DurationVar(&opts.FollowerReadDelay, "datastore-follower-read-delay-duration", 4_800*time.Millisecond, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach driver only)")
	cmd.Flags().Uint16Var(&opts.SplitQueryCount, "datastore-query-userset-batch-size", 1024, "number of usersets after which a relationship query will be split into multiple queries")
	cmd.Flags().IntVar(&opts.MaxRetries, "datastore-max-tx-retries", 10, "number of times a retriable transaction should be retried")
	cmd.Flags().StringVar(&opts.OverlapStrategy, "datastore-tx-overlap-strategy", "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	cmd.Flags().StringVar(&opts.OverlapKey, "datastore-tx-overlap-key", "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")
	cmd.Flags().StringVar(&opts.SpannerCredentialsFile, "datastore-spanner-credentials", "", "path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)")
	cmd.Flags().StringVar(&opts.SpannerEmulatorHost, "datastore-spanner-emulator-host", "", "URI of spanner emulator instance used for development and testing (e.g. localhost:9010)")
	cmd.Flags().StringVar(&opts.TablePrefix, "datastore-mysql-table-prefix", "", "prefix to add to the name of all SpiceDB database tables")

	// disabling stats is only for tests
	cmd.Flags().BoolVar(&opts.DisableStats, "datastore-disable-stats", false, "disable recording relationship counts to the stats table")
	if err := cmd.Flags().MarkHidden("datastore-disable-stats"); err != nil {
		panic("failed to mark flag hidden: " + err.Error())
	}

	cmd.Flags().DurationVar(&opts.LegacyFuzzing, "datastore-revision-fuzzing-duration", -1, "amount of time to advertize stale revisions")
	if err := cmd.Flags().MarkDeprecated("datastore-revision-fuzzing-duration", "please use datastore-revision-quantization-interval instead"); err != nil {
		panic("failed to mark flag deprecated: " + err.Error())
	}
}

func DefaultDatastoreConfig() *Config {
	return &Config{
		GCWindow:               24 * time.Hour,
		RevisionQuantization:   5 * time.Second,
		MaxLifetime:            30 * time.Minute,
		MaxIdleTime:            30 * time.Minute,
		MaxOpenConns:           20,
		MinOpenConns:           10,
		SplitQueryCount:        1024,
		MaxRetries:             50,
		OverlapStrategy:        "prefix",
		HealthCheckPeriod:      30 * time.Second,
		GCInterval:             3 * time.Minute,
		GCMaxOperationTime:     1 * time.Minute,
		WatchBufferLength:      128,
		EnableDatastoreMetrics: true,
		DisableStats:           false,
	}
}

// NewDatastore initializes a datastore given the options
func NewDatastore(options ...ConfigOption) (datastore.Datastore, error) {
	opts := DefaultDatastoreConfig()
	for _, o := range options {
		o(opts)
	}

	if opts.LegacyFuzzing >= 0 {
		log.Warn().Stringer("period", opts.LegacyFuzzing).Msg("deprecated datastore-revision-fuzzing-duration flag specified")
		opts.RevisionQuantization = opts.LegacyFuzzing
	}

	dsBuilder, ok := BuilderForEngine[opts.Engine]
	if !ok {
		return nil, fmt.Errorf("unknown datastore engine type: %s", opts.Engine)
	}
	log.Info().Msgf("using %s datastore engine", opts.Engine)

	ds, err := dsBuilder(*opts)
	if err != nil {
		return nil, err
	}

	if len(opts.BootstrapFiles) > 0 {
		revision, err := ds.HeadRevision(context.Background())
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}

		nsDefs, err := ds.SnapshotReader(revision).ListNamespaces(context.Background())
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
	return crdb.NewCRDBDatastore(
		opts.URI,
		crdb.GCWindow(opts.GCWindow),
		crdb.RevisionQuantization(opts.RevisionQuantization),
		crdb.ConnMaxIdleTime(opts.MaxIdleTime),
		crdb.ConnMaxLifetime(opts.MaxLifetime),
		crdb.ConnHealthCheckInterval(opts.HealthCheckPeriod),
		crdb.MaxOpenConns(opts.MaxOpenConns),
		crdb.MinOpenConns(opts.MinOpenConns),
		crdb.SplitAtUsersetCount(opts.SplitQueryCount),
		crdb.FollowerReadDelay(opts.FollowerReadDelay),
		crdb.MaxRetries(uint8(opts.MaxRetries)),
		crdb.OverlapKey(opts.OverlapKey),
		crdb.OverlapStrategy(opts.OverlapStrategy),
		crdb.WatchBufferLength(opts.WatchBufferLength),
		crdb.DisableStats(opts.DisableStats),
		crdb.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
	)
}

func newPostgresDatastore(opts Config) (datastore.Datastore, error) {
	pgOpts := []postgres.Option{
		postgres.GCWindow(opts.GCWindow),
		postgres.GCEnabled(!opts.ReadOnly),
		postgres.RevisionQuantization(opts.RevisionQuantization),
		postgres.ConnMaxIdleTime(opts.MaxIdleTime),
		postgres.ConnMaxLifetime(opts.MaxLifetime),
		postgres.MaxOpenConns(opts.MaxOpenConns),
		postgres.MinOpenConns(opts.MinOpenConns),
		postgres.SplitAtUsersetCount(opts.SplitQueryCount),
		postgres.HealthCheckPeriod(opts.HealthCheckPeriod),
		postgres.GCInterval(opts.GCInterval),
		postgres.GCMaxOperationTime(opts.GCMaxOperationTime),
		postgres.EnableTracing(),
		postgres.WatchBufferLength(opts.WatchBufferLength),
		postgres.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		postgres.MaxRetries(uint8(opts.MaxRetries)),
	}
	return postgres.NewPostgresDatastore(opts.URI, pgOpts...)
}

func newSpannerDatastore(opts Config) (datastore.Datastore, error) {
	return spanner.NewSpannerDatastore(
		opts.URI,
		spanner.FollowerReadDelay(opts.FollowerReadDelay),
		spanner.GCInterval(opts.GCInterval),
		spanner.GCWindow(opts.GCWindow),
		spanner.GCEnabled(!opts.ReadOnly),
		spanner.CredentialsFile(opts.SpannerCredentialsFile),
		spanner.WatchBufferLength(opts.WatchBufferLength),
		spanner.EmulatorHost(opts.SpannerEmulatorHost),
	)
}

func newMySQLDatastore(opts Config) (datastore.Datastore, error) {
	mysqlOpts := []mysql.Option{
		mysql.GCInterval(opts.GCInterval),
		mysql.GCWindow(opts.GCWindow),
		mysql.GCInterval(opts.GCInterval),
		mysql.GCEnabled(!opts.ReadOnly),
		mysql.ConnMaxIdleTime(opts.MaxIdleTime),
		mysql.ConnMaxLifetime(opts.MaxLifetime),
		mysql.MaxOpenConns(opts.MaxOpenConns),
		mysql.RevisionQuantization(opts.RevisionQuantization),
		mysql.TablePrefix(opts.TablePrefix),
		mysql.WatchBufferLength(opts.WatchBufferLength),
		mysql.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		mysql.MaxRetries(uint8(opts.MaxRetries)),
		mysql.OverrideLockWaitTimeout(1),
	}
	return mysql.NewMySQLDatastore(opts.URI, mysqlOpts...)
}

func newMemoryDatstore(opts Config) (datastore.Datastore, error) {
	log.Warn().Msg("in-memory datastore is not persistent and not feasible to run in a high availability fashion")
	return memdb.NewMemdbDatastore(opts.WatchBufferLength, opts.RevisionQuantization, opts.GCWindow)
}
