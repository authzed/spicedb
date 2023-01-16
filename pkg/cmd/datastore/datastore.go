package datastore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/datastore/spanner"
	log "github.com/authzed/spicedb/internal/logging"
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
	BootstrapTimeout   time.Duration

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

	// Migrations
	MigrationPhase string
}

// RegisterDatastoreFlags adds datastore flags to a cobra command.
func RegisterDatastoreFlags(cmd *cobra.Command, opts *Config) error {
	return RegisterDatastoreFlagsWithPrefix(cmd.Flags(), "", opts)
}

// RegisterDatastoreFlagsWithPrefix adds datastore flags to a cobra command, with each flag prefixed with the provided
// prefix argument. If left empty, the datastore flags are not prefixed.
func RegisterDatastoreFlagsWithPrefix(flagSet *pflag.FlagSet, prefix string, opts *Config) error {
	if prefix != "" {
		prefix = prefix + "-"
	}
	flagName := func(flag string) string {
		return fmt.Sprintf("%s%s", prefix, flag)
	}
	defaults := DefaultDatastoreConfig()

	flagSet.StringVar(&opts.Engine, flagName("datastore-engine"), defaults.Engine, fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	flagSet.StringVar(&opts.URI, flagName("datastore-conn-uri"), defaults.URI, `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	flagSet.IntVar(&opts.MaxOpenConns, flagName("datastore-conn-max-open"), defaults.MaxOpenConns, "number of concurrent connections open in a remote datastore's connection pool")
	flagSet.IntVar(&opts.MinOpenConns, flagName("datastore-conn-min-open"), defaults.MinOpenConns, "number of minimum concurrent connections open in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.MaxLifetime, flagName("datastore-conn-max-lifetime"), defaults.MaxLifetime, "maximum amount of time a connection can live in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.MaxIdleTime, flagName("datastore-conn-max-idletime"), defaults.MaxIdleTime, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.HealthCheckPeriod, flagName("datastore-conn-healthcheck-interval"), defaults.HealthCheckPeriod, "time between a remote datastore's connection pool health checks")
	flagSet.DurationVar(&opts.GCWindow, flagName("datastore-gc-window"), defaults.GCWindow, "amount of time before revisions are garbage collected")
	flagSet.DurationVar(&opts.GCInterval, flagName("datastore-gc-interval"), defaults.GCInterval, "amount of time between passes of garbage collection (postgres driver only)")
	flagSet.DurationVar(&opts.GCMaxOperationTime, flagName("datastore-gc-max-operation-time"), defaults.GCMaxOperationTime, "maximum amount of time a garbage collection pass can operate before timing out (postgres driver only)")
	flagSet.DurationVar(&opts.RevisionQuantization, flagName("datastore-revision-quantization-interval"), defaults.RevisionQuantization, "boundary interval to which to round the quantized revision")
	flagSet.BoolVar(&opts.ReadOnly, flagName("datastore-readonly"), defaults.ReadOnly, "set the service to read-only mode")
	flagSet.StringSliceVar(&opts.BootstrapFiles, flagName("datastore-bootstrap-files"), defaults.BootstrapFiles, "bootstrap data yaml files to load")
	flagSet.BoolVar(&opts.BootstrapOverwrite, flagName("datastore-bootstrap-overwrite"), defaults.BootstrapOverwrite, "overwrite any existing data with bootstrap data")
	flagSet.DurationVar(&opts.BootstrapTimeout, flagName("datastore-bootstrap-timeout"), defaults.BootstrapTimeout, "maximum duration before timeout for the bootstrap data to be written")
	flagSet.BoolVar(&opts.RequestHedgingEnabled, flagName("datastore-request-hedging"), defaults.RequestHedgingEnabled, "enable request hedging")
	flagSet.DurationVar(&opts.RequestHedgingInitialSlowValue, flagName("datastore-request-hedging-initial-slow-value"), defaults.RequestHedgingInitialSlowValue, "initial value to use for slow datastore requests, before statistics have been collected")
	flagSet.Uint64Var(&opts.RequestHedgingMaxRequests, flagName("datastore-request-hedging-max-requests"), defaults.RequestHedgingMaxRequests, "maximum number of historical requests to consider")
	flagSet.Float64Var(&opts.RequestHedgingQuantile, flagName("datastore-request-hedging-quantile"), defaults.RequestHedgingQuantile, "quantile of historical datastore request time over which a request will be considered slow")
	flagSet.BoolVar(&opts.EnableDatastoreMetrics, flagName("datastore-prometheus-metrics"), defaults.EnableDatastoreMetrics, "set to false to disabled prometheus metrics from the datastore")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	flagSet.DurationVar(&opts.FollowerReadDelay, flagName("datastore-follower-read-delay-duration"), 4_800*time.Millisecond, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach driver only)")
	flagSet.Uint16Var(&opts.SplitQueryCount, flagName("datastore-query-userset-batch-size"), 1024, "number of usersets after which a relationship query will be split into multiple queries")
	flagSet.IntVar(&opts.MaxRetries, flagName("datastore-max-tx-retries"), 10, "number of times a retriable transaction should be retried")
	flagSet.StringVar(&opts.OverlapStrategy, flagName("datastore-tx-overlap-strategy"), "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	flagSet.StringVar(&opts.OverlapKey, flagName("datastore-tx-overlap-key"), "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")
	flagSet.StringVar(&opts.SpannerCredentialsFile, flagName("datastore-spanner-credentials"), "", "path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)")
	flagSet.StringVar(&opts.SpannerEmulatorHost, flagName("datastore-spanner-emulator-host"), "", "URI of spanner emulator instance used for development and testing (e.g. localhost:9010)")
	flagSet.StringVar(&opts.TablePrefix, flagName("datastore-mysql-table-prefix"), "", "prefix to add to the name of all SpiceDB database tables")
	flagSet.StringVar(&opts.MigrationPhase, flagName("datastore-migration-phase"), "", "datastore-specific flag that should be used to signal to a datastore which phase of a multi-step migration it is in")
	flagSet.Uint16Var(&opts.WatchBufferLength, flagName("datastore-watch-buffer-length"), 1024, "how many events the watch buffer should queue before forcefully disconnecting reader")

	// disabling stats is only for tests
	flagSet.BoolVar(&opts.DisableStats, flagName("datastore-disable-stats"), false, "disable recording relationship counts to the stats table")
	if err := flagSet.MarkHidden(flagName("datastore-disable-stats")); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	flagSet.DurationVar(&opts.LegacyFuzzing, flagName("datastore-revision-fuzzing-duration"), -1, "amount of time to advertize stale revisions")
	if err := flagSet.MarkDeprecated(flagName("datastore-revision-fuzzing-duration"), "please use datastore-revision-quantization-interval instead"); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}

	return nil
}

func DefaultDatastoreConfig() *Config {
	return &Config{
		Engine:                         MemoryEngine,
		GCWindow:                       24 * time.Hour,
		LegacyFuzzing:                  -1,
		RevisionQuantization:           5 * time.Second,
		MaxLifetime:                    30 * time.Minute,
		MaxIdleTime:                    30 * time.Minute,
		MaxOpenConns:                   20,
		MinOpenConns:                   10,
		SplitQueryCount:                1024,
		ReadOnly:                       false,
		MaxRetries:                     10,
		OverlapKey:                     "key",
		OverlapStrategy:                "static",
		HealthCheckPeriod:              30 * time.Second,
		GCInterval:                     3 * time.Minute,
		GCMaxOperationTime:             1 * time.Minute,
		WatchBufferLength:              1024,
		EnableDatastoreMetrics:         true,
		DisableStats:                   false,
		BootstrapFiles:                 []string{},
		BootstrapTimeout:               10 * time.Second,
		BootstrapOverwrite:             false,
		RequestHedgingEnabled:          true,
		RequestHedgingInitialSlowValue: 10000000,
		RequestHedgingMaxRequests:      1_000_000,
		RequestHedgingQuantile:         0.95,
		SpannerCredentialsFile:         "",
		SpannerEmulatorHost:            "",
		TablePrefix:                    "",
		MigrationPhase:                 "",
		FollowerReadDelay:              4_800 * time.Millisecond,
	}
}

// NewDatastore initializes a datastore given the options
func NewDatastore(ctx context.Context, options ...ConfigOption) (datastore.Datastore, error) {
	opts := DefaultDatastoreConfig()
	for _, o := range options {
		o(opts)
	}

	if opts.LegacyFuzzing >= 0 {
		log.Ctx(ctx).Warn().Stringer("period", opts.LegacyFuzzing).Msg("deprecated datastore-revision-fuzzing-duration flag specified")
		opts.RevisionQuantization = opts.LegacyFuzzing
	}

	dsBuilder, ok := BuilderForEngine[opts.Engine]
	if !ok {
		return nil, fmt.Errorf("unknown datastore engine type: %s", opts.Engine)
	}
	log.Ctx(ctx).Info().Msgf("using %s datastore engine", opts.Engine)

	ds, err := dsBuilder(*opts)
	if err != nil {
		return nil, err
	}

	if len(opts.BootstrapFiles) > 0 {
		ctx, cancel := context.WithTimeout(ctx, opts.BootstrapTimeout)
		defer cancel()

		revision, err := ds.HeadRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}

		nsDefs, err := ds.SnapshotReader(revision).ListNamespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}
		if opts.BootstrapOverwrite || len(nsDefs) == 0 {
			log.Ctx(ctx).Info().Msg("initializing datastore from bootstrap files")
			_, _, err = validationfile.PopulateFromFiles(ctx, ds, opts.BootstrapFiles)
			if err != nil {
				return nil, fmt.Errorf("failed to load bootstrap files: %w", err)
			}
		} else {
			return nil, errors.New("cannot apply bootstrap data: schema or tuples already exist in the datastore. Delete existing data or set the flag --datastore-bootstrap-overwrite=true")
		}
	}

	if opts.RequestHedgingEnabled {
		log.Ctx(ctx).Info().
			Stringer("initialSlowRequest", opts.RequestHedgingInitialSlowValue).
			Uint64("maxRequests", opts.RequestHedgingMaxRequests).
			Float64("hedgingQuantile", opts.RequestHedgingQuantile).
			Msg("request hedging enabled")

		hds, err := proxy.NewHedgingProxy(
			ds,
			opts.RequestHedgingInitialSlowValue,
			opts.RequestHedgingMaxRequests,
			opts.RequestHedgingQuantile,
		)
		if err != nil {
			return nil, fmt.Errorf("error in configuring request hedging: %w", err)
		}
		ds = hds
	}

	if opts.ReadOnly {
		log.Ctx(ctx).Warn().Msg("setting the datastore to read-only")
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
		postgres.MigrationPhase(opts.MigrationPhase),
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
		mysql.GCMaxOperationTime(opts.GCMaxOperationTime),
		mysql.ConnMaxIdleTime(opts.MaxIdleTime),
		mysql.ConnMaxLifetime(opts.MaxLifetime),
		mysql.MaxOpenConns(opts.MaxOpenConns),
		mysql.RevisionQuantization(opts.RevisionQuantization),
		mysql.TablePrefix(opts.TablePrefix),
		mysql.WatchBufferLength(opts.WatchBufferLength),
		mysql.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		mysql.MaxRetries(uint8(opts.MaxRetries)),
		mysql.OverrideLockWaitTimeout(1),
		mysql.SplitAtUsersetCount(opts.SplitQueryCount),
	}
	return mysql.NewMySQLDatastore(opts.URI, mysqlOpts...)
}

func newMemoryDatstore(opts Config) (datastore.Datastore, error) {
	log.Warn().Msg("in-memory datastore is not persistent and not feasible to run in a high availability fashion")
	return memdb.NewMemdbDatastore(opts.WatchBufferLength, opts.RevisionQuantization, opts.GCWindow)
}
