package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast"
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

type engineBuilderFunc func(ctx context.Context, options Config) (datastore.Datastore, error)

const (
	MaxReplicaCount          = 16
	DefaultFollowerReadDelay = 4_800 * time.Millisecond
)

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

//go:generate go run github.com/ecordell/optgen -output zz_generated.connpool.options.go . ConnPoolConfig
type ConnPoolConfig struct {
	MaxIdleTime         time.Duration `debugmap:"visible"`
	MaxLifetime         time.Duration `debugmap:"visible"`
	MaxLifetimeJitter   time.Duration `debugmap:"visible"`
	MaxOpenConns        int           `debugmap:"visible"`
	MinOpenConns        int           `debugmap:"visible"`
	HealthCheckInterval time.Duration `debugmap:"visible"`
}

func DefaultReadConnPool() *ConnPoolConfig {
	return &ConnPoolConfig{
		MaxLifetime:         30 * time.Minute,
		MaxIdleTime:         30 * time.Minute,
		MaxOpenConns:        20,
		MinOpenConns:        20,
		HealthCheckInterval: 30 * time.Second,
	}
}

func DefaultWriteConnPool() *ConnPoolConfig {
	cfg := DefaultReadConnPool()
	cfg.MaxOpenConns = cfg.MaxOpenConns / 2
	cfg.MinOpenConns = cfg.MinOpenConns / 2
	return cfg
}

func RegisterConnPoolFlagsWithPrefix(flagSet *pflag.FlagSet, prefix string, defaults, opts *ConnPoolConfig) {
	if prefix != "" {
		prefix = prefix + "-"
	}
	flagName := func(flag string) string {
		return prefix + flag
	}

	flagSet.IntVar(&opts.MaxOpenConns, flagName("max-open"), defaults.MaxOpenConns, "number of concurrent connections open in a remote datastore's connection pool")
	flagSet.IntVar(&opts.MinOpenConns, flagName("min-open"), defaults.MinOpenConns, "number of minimum concurrent connections open in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.MaxLifetime, flagName("max-lifetime"), defaults.MaxLifetime, "maximum amount of time a connection can live in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.MaxLifetimeJitter, flagName("max-lifetime-jitter"), defaults.MaxLifetimeJitter, "waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime)")
	flagSet.DurationVar(&opts.MaxIdleTime, flagName("max-idletime"), defaults.MaxIdleTime, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.HealthCheckInterval, flagName("healthcheck-interval"), defaults.HealthCheckInterval, "amount of time between connection health checks in a remote datastore's connection pool")
}

func deprecateUnifiedConnFlags(flagSet *pflag.FlagSet) {
	const warning = "connection pooling has been split into read and write pools"
	_ = flagSet.MarkDeprecated("datastore-conn-max-open", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-min-open", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-max-lifetime", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-max-idletime", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-healthcheck-interval", warning)
}

//go:generate go run github.com/ecordell/optgen -sensitive-field-name-matches uri,secure -output zz_generated.options.go . Config
type Config struct {
	Engine                      string        `debugmap:"visible"`
	URI                         string        `debugmap:"sensitive"`
	GCWindow                    time.Duration `debugmap:"visible"`
	LegacyFuzzing               time.Duration `debugmap:"visible"`
	RevisionQuantization        time.Duration `debugmap:"visible"`
	MaxRevisionStalenessPercent float64       `debugmap:"visible"`
	CredentialsProviderName     string        `debugmap:"visible"`
	FilterMaximumIDCount        uint16        `debugmap:"hidden" default:"100"`

	// Options
	ReadConnPool                   ConnPoolConfig `debugmap:"visible"`
	WriteConnPool                  ConnPoolConfig `debugmap:"visible"`
	ReadOnly                       bool           `debugmap:"visible"`
	EnableDatastoreMetrics         bool           `debugmap:"visible"`
	DisableStats                   bool           `debugmap:"visible"`
	IncludeQueryParametersInTraces bool           `debugmap:"visible"`

	// Read Replicas
	ReadReplicaConnPool ConnPoolConfig `debugmap:"visible"`
	// this holds values from the old flag prefix in case they are used
	OldReadReplicaConnPool             ConnPoolConfig `debugmap:"hidden"`
	ReadReplicaURIs                    []string       `debugmap:"sensitive"`
	ReadReplicaCredentialsProviderName string         `debugmap:"visible"`

	// Bootstrap
	BootstrapFiles        []string          `debugmap:"visible-format"`
	BootstrapFileContents map[string][]byte `debugmap:"visible"`
	BootstrapOverwrite    bool              `debugmap:"visible"`
	BootstrapTimeout      time.Duration     `debugmap:"visible"`

	// Hedging
	RequestHedgingEnabled          bool          `debugmap:"visible"`
	RequestHedgingInitialSlowValue time.Duration `debugmap:"visible"`
	RequestHedgingMaxRequests      uint64        `debugmap:"visible"`
	RequestHedgingQuantile         float64       `debugmap:"visible"`

	// CRDB
	FollowerReadDelay         time.Duration `debugmap:"visible"`
	MaxRetries                int           `debugmap:"visible"`
	OverlapKey                string        `debugmap:"visible"`
	OverlapStrategy           string        `debugmap:"visible"`
	EnableConnectionBalancing bool          `debugmap:"visible"`
	ConnectRate               time.Duration `debugmap:"visible"`

	// Postgres
	GCInterval         time.Duration `debugmap:"visible"`
	GCMaxOperationTime time.Duration `debugmap:"visible"`

	// Spanner
	SpannerCredentialsFile        string `debugmap:"visible"`
	SpannerCredentialsJSON        []byte `debugmap:"sensitive"`
	SpannerEmulatorHost           string `debugmap:"visible"`
	SpannerMinSessions            uint64 `debugmap:"visible"`
	SpannerMaxSessions            uint64 `debugmap:"visible"`
	SpannerDatastoreMetricsOption string `debugmap:"visible"`

	// MySQL
	TablePrefix string `debugmap:"visible"`

	// Relationship Integrity
	RelationshipIntegrityEnabled     bool            `debugmap:"visible"`
	RelationshipIntegrityCurrentKey  RelIntegrityKey `debugmap:"visible"`
	RelationshipIntegrityExpiredKeys []string        `debugmap:"visible"`

	// Internal
	WatchBufferLength       uint16        `debugmap:"visible"`
	WatchBufferWriteTimeout time.Duration `debugmap:"visible"`
	WatchConnectTimeout     time.Duration `debugmap:"visible"`

	// Migrations
	MigrationPhase    string   `debugmap:"visible"`
	AllowedMigrations []string `debugmap:"visible"`

	// Expermimental
	ExperimentalColumnOptimization           bool `debugmap:"visible"`
	EnableExperimentalRelationshipExpiration bool `debugmap:"visible"`
	EnableRevisionHeartbeat                  bool `debugmap:"visible"`
}

//go:generate go run github.com/ecordell/optgen -sensitive-field-name-matches uri,secure -output zz_generated.relintegritykey.options.go . RelIntegrityKey
type RelIntegrityKey struct {
	KeyID       string `debugmap:"visible"`
	KeyFilename string `debugmap:"visible"`
}

// RegisterDatastoreFlags adds datastore flags to a cobra command.
func RegisterDatastoreFlags(flagset *pflag.FlagSet, opts *Config) error {
	return RegisterDatastoreFlagsWithPrefix(flagset, "", opts)
}

// RegisterDatastoreFlagsWithPrefix adds datastore flags to a cobra command, with each flag prefixed with the provided
// prefix argument. If left empty, the datastore flags are not prefixed.
func RegisterDatastoreFlagsWithPrefix(flagSet *pflag.FlagSet, prefix string, opts *Config) error {
	if prefix != "" {
		prefix = prefix + "-"
	}
	flagName := func(flag string) string {
		return prefix + flag
	}
	defaults := DefaultDatastoreConfig()

	flagSet.StringVar(&opts.Engine, flagName("datastore-engine"), defaults.Engine, fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	flagSet.StringVar(&opts.URI, flagName("datastore-conn-uri"), defaults.URI, `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	flagSet.StringVar(&opts.CredentialsProviderName, flagName("datastore-credentials-provider-name"), defaults.CredentialsProviderName, fmt.Sprintf(`retrieve datastore credentials dynamically using (%s)`, datastore.CredentialsProviderOptions()))

	flagSet.StringArrayVar(&opts.ReadReplicaURIs, flagName("datastore-read-replica-conn-uri"), []string{}, "connection string used by remote datastores for read replicas (e.g. \"postgres://postgres:password@localhost:5432/spicedb\"). Only supported for postgres and mysql.")
	flagSet.StringVar(&opts.ReadReplicaCredentialsProviderName, flagName("datastore-read-replica-credentials-provider-name"), defaults.CredentialsProviderName, fmt.Sprintf(`retrieve datastore credentials dynamically using (%s)`, datastore.CredentialsProviderOptions()))

	var legacyConnPool ConnPoolConfig
	RegisterConnPoolFlagsWithPrefix(flagSet, "datastore-conn", DefaultReadConnPool(), &legacyConnPool)
	deprecateUnifiedConnFlags(flagSet)
	RegisterConnPoolFlagsWithPrefix(flagSet, "datastore-conn-pool-read", &legacyConnPool, &opts.ReadConnPool)
	RegisterConnPoolFlagsWithPrefix(flagSet, "datastore-conn-pool-write", DefaultWriteConnPool(), &opts.WriteConnPool)

	// read replica prefix changed but we retain backward-compatibility
	newReadReplicaPrefix := "datastore-read-replica-conn-pool-read"
	oldReadReplicaPrefix := "datastore-read-replica-conn-pool"
	RegisterConnPoolFlagsWithPrefix(flagSet, newReadReplicaPrefix, DefaultReadConnPool(), &opts.ReadReplicaConnPool)
	RegisterConnPoolFlagsWithPrefix(flagSet, oldReadReplicaPrefix, DefaultReadConnPool(), &opts.OldReadReplicaConnPool)

	warning := fmt.Sprintf("please use the flags with the prefix %q instead of %q", newReadReplicaPrefix, oldReadReplicaPrefix)
	for _, flag := range []string{"max-open", "min-open", "max-lifetime", "max-lifetime-jitter", "max-idletime", "healthcheck-interval"} {
		if err := flagSet.MarkDeprecated(oldReadReplicaPrefix+"-"+flag, warning); err != nil {
			return fmt.Errorf("failed to mark flag as deprecated: %w", err)
		}
		if err := flagSet.MarkHidden(oldReadReplicaPrefix + "-" + flag); err != nil {
			return fmt.Errorf("failed to mark flag as hidden: %w", err)
		}
	}

	normalizeFunc := flagSet.GetNormalizeFunc()
	flagSet.SetNormalizeFunc(func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		if normalizeFunc != nil {
			name = string(normalizeFunc(f, name))
		}
		if strings.HasPrefix(name, "datastore-connpool") {
			return pflag.NormalizedName(strings.ReplaceAll(name, "connpool", "conn-pool"))
		}
		return pflag.NormalizedName(name)
	})

	var unusedSplitQueryCount uint16

	flagSet.DurationVar(&opts.GCWindow, flagName("datastore-gc-window"), defaults.GCWindow, "amount of time before revisions are garbage collected")
	flagSet.DurationVar(&opts.GCInterval, flagName("datastore-gc-interval"), defaults.GCInterval, "amount of time between passes of garbage collection (postgres driver only)")
	flagSet.DurationVar(&opts.GCMaxOperationTime, flagName("datastore-gc-max-operation-time"), defaults.GCMaxOperationTime, "maximum amount of time a garbage collection pass can operate before timing out (postgres driver only)")
	flagSet.DurationVar(&opts.RevisionQuantization, flagName("datastore-revision-quantization-interval"), defaults.RevisionQuantization, "boundary interval to which to round the quantized revision")
	flagSet.Float64Var(&opts.MaxRevisionStalenessPercent, flagName("datastore-revision-quantization-max-staleness-percent"), defaults.MaxRevisionStalenessPercent, "float percentage (where 1 = 100%) of the revision quantization interval where we may opt to select a stale revision for performance reasons. Defaults to 0.1 (representing 10%)")
	flagSet.BoolVar(&opts.ReadOnly, flagName("datastore-readonly"), defaults.ReadOnly, "set the service to read-only mode")
	flagSet.StringSliceVar(&opts.BootstrapFiles, flagName("datastore-bootstrap-files"), defaults.BootstrapFiles, "bootstrap data yaml files to load")
	flagSet.BoolVar(&opts.BootstrapOverwrite, flagName("datastore-bootstrap-overwrite"), defaults.BootstrapOverwrite, "overwrite any existing data with bootstrap data (this can be quite slow)")
	flagSet.DurationVar(&opts.BootstrapTimeout, flagName("datastore-bootstrap-timeout"), defaults.BootstrapTimeout, "maximum duration before timeout for the bootstrap data to be written")
	flagSet.BoolVar(&opts.RequestHedgingEnabled, flagName("datastore-request-hedging"), defaults.RequestHedgingEnabled, "enable request hedging")
	flagSet.DurationVar(&opts.RequestHedgingInitialSlowValue, flagName("datastore-request-hedging-initial-slow-value"), defaults.RequestHedgingInitialSlowValue, "initial value to use for slow datastore requests, before statistics have been collected")
	flagSet.Uint64Var(&opts.RequestHedgingMaxRequests, flagName("datastore-request-hedging-max-requests"), defaults.RequestHedgingMaxRequests, "maximum number of historical requests to consider")
	flagSet.Float64Var(&opts.RequestHedgingQuantile, flagName("datastore-request-hedging-quantile"), defaults.RequestHedgingQuantile, "quantile of historical datastore request time over which a request will be considered slow")
	flagSet.BoolVar(&opts.EnableDatastoreMetrics, flagName("datastore-prometheus-metrics"), defaults.EnableDatastoreMetrics, "set to false to disabled metrics from the datastore (do not use for Spanner; setting to false will disable metrics to the configured metrics store in Spanner)")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	flagSet.DurationVar(&opts.FollowerReadDelay, flagName("datastore-follower-read-delay-duration"), DefaultFollowerReadDelay, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach and spanner drivers only) or read replicas (postgres and mysql drivers only)")
	flagSet.IntVar(&opts.MaxRetries, flagName("datastore-max-tx-retries"), 10, "number of times a retriable transaction should be retried")
	flagSet.StringVar(&opts.OverlapStrategy, flagName("datastore-tx-overlap-strategy"), "static", `strategy to generate transaction overlap keys ("request", "prefix", "static", "insecure") (cockroach driver only - see https://spicedb.dev/d/crdb-overlap for details)"`)
	flagSet.StringVar(&opts.OverlapKey, flagName("datastore-tx-overlap-key"), "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")
	flagSet.BoolVar(&opts.EnableConnectionBalancing, flagName("datastore-connection-balancing"), defaults.EnableConnectionBalancing, "enable connection balancing between database nodes (cockroach driver only)")
	flagSet.DurationVar(&opts.ConnectRate, flagName("datastore-connect-rate"), 100*time.Millisecond, "rate at which new connections are allowed to the datastore (at a rate of 1/duration) (cockroach driver only)")
	flagSet.StringVar(&opts.SpannerCredentialsFile, flagName("datastore-spanner-credentials"), "", "path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)")
	flagSet.StringVar(&opts.SpannerEmulatorHost, flagName("datastore-spanner-emulator-host"), "", "URI of spanner emulator instance used for development and testing (e.g. localhost:9010)")
	flagSet.Uint64Var(&opts.SpannerMinSessions, flagName("datastore-spanner-min-sessions"), 100, "minimum number of sessions across all Spanner gRPC connections the client can have at a given time")
	flagSet.Uint64Var(&opts.SpannerMaxSessions, flagName("datastore-spanner-max-sessions"), 400, "maximum number of sessions across all Spanner gRPC connections the client can have at a given time")
	flagSet.StringVar(&opts.SpannerDatastoreMetricsOption, flagName("datastore-spanner-metrics"), "otel", `configure the metrics that are emitted by the Spanner datastore ("none", "native", "otel", "deprecated-prometheus")`)
	flagSet.StringVar(&opts.TablePrefix, flagName("datastore-mysql-table-prefix"), "", "prefix to add to the name of all SpiceDB database tables")
	flagSet.StringVar(&opts.MigrationPhase, flagName("datastore-migration-phase"), "", "datastore-specific flag that should be used to signal to a datastore which phase of a multi-step migration it is in")
	flagSet.StringArrayVar(&opts.AllowedMigrations, flagName("datastore-allowed-migrations"), []string{}, "migration levels that will not fail the health check (in addition to the current head migration)")
	flagSet.Uint16Var(&opts.WatchBufferLength, flagName("datastore-watch-buffer-length"), 1024, "how large the watch buffer should be before blocking")
	flagSet.DurationVar(&opts.WatchBufferWriteTimeout, flagName("datastore-watch-buffer-write-timeout"), 1*time.Second, "how long the watch buffer should queue before forcefully disconnecting the reader")
	flagSet.DurationVar(&opts.WatchConnectTimeout, flagName("datastore-watch-connect-timeout"), 1*time.Second, "how long the watch connection should wait before timing out (cockroachdb driver only)")
	flagSet.BoolVar(&opts.IncludeQueryParametersInTraces, flagName("datastore-include-query-parameters-in-traces"), false, "include query parameters in traces (postgres and CRDB drivers only)")

	flagSet.BoolVar(&opts.RelationshipIntegrityEnabled, flagName("datastore-relationship-integrity-enabled"), false, "enables relationship integrity checks. only supported on CRDB")
	flagSet.StringVar(&opts.RelationshipIntegrityCurrentKey.KeyID, flagName("datastore-relationship-integrity-current-key-id"), "", "current key id for relationship integrity checks")
	flagSet.StringVar(&opts.RelationshipIntegrityCurrentKey.KeyFilename, flagName("datastore-relationship-integrity-current-key-filename"), "", "current key filename for relationship integrity checks")
	flagSet.StringArrayVar(&opts.RelationshipIntegrityExpiredKeys, flagName("datastore-relationship-integrity-expired-keys"), []string{}, "config for expired keys for relationship integrity checks")

	// disabling stats is only for tests
	flagSet.BoolVar(&opts.DisableStats, flagName("datastore-disable-stats"), false, "disable recording relationship counts to the stats table")
	if err := flagSet.MarkHidden(flagName("datastore-disable-stats")); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	flagSet.DurationVar(&opts.LegacyFuzzing, flagName("datastore-revision-fuzzing-duration"), -1, "amount of time to advertize stale revisions")
	if err := flagSet.MarkDeprecated(flagName("datastore-revision-fuzzing-duration"), "please use datastore-revision-quantization-interval instead"); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}

	flagSet.Uint16Var(&unusedSplitQueryCount, flagName("datastore-query-userset-batch-size"), 1024, "number of usersets after which a relationship query will be split into multiple queries")
	if err := flagSet.MarkHidden(flagName("datastore-query-userset-batch-size")); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	flagSet.BoolVar(&opts.ExperimentalColumnOptimization, flagName("datastore-experimental-column-optimization"), false, "enable experimental column optimization")

	return nil
}

func DefaultDatastoreConfig() *Config {
	return &Config{
		Engine:                                   MemoryEngine,
		GCWindow:                                 24 * time.Hour,
		LegacyFuzzing:                            -1,
		RevisionQuantization:                     5 * time.Second,
		MaxRevisionStalenessPercent:              .1, // 10%
		ReadConnPool:                             *DefaultReadConnPool(),
		WriteConnPool:                            *DefaultWriteConnPool(),
		ReadReplicaConnPool:                      *DefaultReadConnPool(),
		OldReadReplicaConnPool:                   *DefaultReadConnPool(),
		ReadReplicaURIs:                          []string{},
		ReadOnly:                                 false,
		MaxRetries:                               10,
		OverlapKey:                               "key",
		OverlapStrategy:                          "static",
		ConnectRate:                              100 * time.Millisecond,
		EnableConnectionBalancing:                true,
		GCInterval:                               3 * time.Minute,
		GCMaxOperationTime:                       1 * time.Minute,
		WatchBufferLength:                        1024,
		WatchBufferWriteTimeout:                  1 * time.Second,
		WatchConnectTimeout:                      1 * time.Second,
		EnableDatastoreMetrics:                   true,
		DisableStats:                             false,
		BootstrapFiles:                           []string{},
		BootstrapTimeout:                         10 * time.Second,
		BootstrapOverwrite:                       false,
		RequestHedgingEnabled:                    false,
		RequestHedgingInitialSlowValue:           10000000,
		RequestHedgingMaxRequests:                1_000_000,
		RequestHedgingQuantile:                   0.95,
		SpannerCredentialsFile:                   "",
		SpannerEmulatorHost:                      "",
		TablePrefix:                              "",
		MigrationPhase:                           "",
		FollowerReadDelay:                        DefaultFollowerReadDelay,
		SpannerMinSessions:                       100,
		SpannerMaxSessions:                       400,
		FilterMaximumIDCount:                     100,
		SpannerDatastoreMetricsOption:            spanner.DatastoreMetricsOptionOpenTelemetry,
		RelationshipIntegrityEnabled:             false,
		RelationshipIntegrityCurrentKey:          RelIntegrityKey{},
		RelationshipIntegrityExpiredKeys:         []string{},
		AllowedMigrations:                        []string{},
		ExperimentalColumnOptimization:           false,
		IncludeQueryParametersInTraces:           false,
		EnableExperimentalRelationshipExpiration: false,
	}
}

// NewDatastore initializes a datastore given the options
func NewDatastore(ctx context.Context, options ...ConfigOption) (datastore.Datastore, error) {
	opts := DefaultDatastoreConfig()
	for _, o := range options {
		o(opts)
	}

	if (opts.Engine == PostgresEngine || opts.Engine == MySQLEngine) && opts.FollowerReadDelay == DefaultFollowerReadDelay {
		// Set the default follower read delay for postgres and mysql to 0 -
		// this should only be set if read replicas are used.
		opts.FollowerReadDelay = 0
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

	ds, err := dsBuilder(ctx, *opts)
	if err != nil {
		return nil, err
	}

	if len(opts.BootstrapFiles) > 0 || len(opts.BootstrapFileContents) > 0 {
		ctx, cancel := context.WithTimeout(ctx, opts.BootstrapTimeout)
		defer cancel()

		revision, err := ds.HeadRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}

		nsDefs, err := ds.SnapshotReader(revision).ListAllNamespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to determine datastore state before applying bootstrap data: %w", err)
		}

		if opts.BootstrapOverwrite {
			log.Ctx(ctx).Info().Msg("deleting existing data before applying bootstrap data (this may take a bit)")
			if err := datastore.DeleteAllData(ctx, ds); err != nil {
				return nil, fmt.Errorf("failed to delete existing data before applying bootstrap data: %w", err)
			}
			log.Ctx(ctx).Info().Msg("deleted existing data before applying bootstrap data")
		} else if len(nsDefs) > 0 {
			return nil, errors.New("cannot apply bootstrap data: schema or tuples already exist in the datastore. Delete existing data or set the flag --datastore-bootstrap-overwrite=true")
		}

		log.Ctx(ctx).Info().Strs("files", opts.BootstrapFiles).Msg("initializing datastore from bootstrap files")

		if len(opts.BootstrapFiles) > 0 {
			_, _, err = validationfile.PopulateFromFiles(ctx, ds, opts.BootstrapFiles)
			if err != nil {
				return nil, fmt.Errorf("failed to load bootstrap files: %w", err)
			}
		}

		if len(opts.BootstrapFileContents) > 0 {
			_, _, err = validationfile.PopulateFromFilesContents(ctx, ds, opts.BootstrapFileContents)
			if err != nil {
				return nil, fmt.Errorf("failed to load bootstrap file contents: %w", err)
			}
		}
		log.Ctx(ctx).Info().Strs("files", opts.BootstrapFiles).Msg("completed datastore initialization from bootstrap files")
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
		log.Ctx(ctx).Info().Msg("setting the datastore to read-only")
		ds = proxy.NewReadonlyDatastore(ds)
	}

	if opts.RelationshipIntegrityEnabled {
		log.Ctx(ctx).Info().Msg("enabling relationship integrity checks")

		keyBytes, err := os.ReadFile(opts.RelationshipIntegrityCurrentKey.KeyFilename)
		if err != nil {
			return nil, fmt.Errorf("error in opening current key file: %w", err)
		}

		currentKey := proxy.KeyConfig{
			ID:    opts.RelationshipIntegrityCurrentKey.KeyID,
			Bytes: keyBytes,
		}

		expiredKeys, err := readExpiredKeys(opts.RelationshipIntegrityExpiredKeys)
		if err != nil {
			return nil, fmt.Errorf("error in reading expired keys: %w", err)
		}

		wrapped, err := proxy.NewRelationshipIntegrityProxy(ds, currentKey, expiredKeys)
		if err != nil {
			return nil, fmt.Errorf("error in configuring relationship integrity checks: %w", err)
		}

		ds = wrapped
	}

	return ds, nil
}

type expiredKeyStruct struct {
	KeyID       string    `json:"key_id"`
	KeyFilename string    `json:"key_filename"`
	ExpiredAt   time.Time `json:"expired_at"`
}

func readExpiredKeys(expiredKeyStrings []string) ([]proxy.KeyConfig, error) {
	expiredKeys := make([]proxy.KeyConfig, 0, len(expiredKeyStrings))
	for index, keyString := range expiredKeyStrings {
		key := expiredKeyStruct{}
		err := json.Unmarshal([]byte(keyString), &key)
		if err != nil {
			return nil, fmt.Errorf("error in unmarshalling expired key #%d: %w", index+1, err)
		}

		keyBytes, err := os.ReadFile(key.KeyFilename)
		if err != nil {
			return nil, fmt.Errorf("error in opening current key file: %w", err)
		}

		expiredAt := key.ExpiredAt
		expiredKey := proxy.KeyConfig{
			ID:        key.KeyID,
			Bytes:     keyBytes,
			ExpiredAt: &expiredAt,
		}
		expiredKeys = append(expiredKeys, expiredKey)
	}

	return expiredKeys, nil
}

func newCRDBDatastore(ctx context.Context, opts Config) (datastore.Datastore, error) {
	if len(opts.ReadReplicaURIs) > 0 {
		return nil, errors.New("read replicas are not supported for the CockroachDB datastore engine")
	}

	maxRetries, err := safecast.ToUint8(opts.MaxRetries)
	if err != nil {
		return nil, errors.New("max-retries could not be cast to uint8")
	}

	return crdb.NewCRDBDatastore(
		ctx,
		opts.URI,
		crdb.GCWindow(opts.GCWindow),
		crdb.RevisionQuantization(opts.RevisionQuantization),
		crdb.MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		crdb.ReadConnsMaxOpen(opts.ReadConnPool.MaxOpenConns),
		crdb.ReadConnsMinOpen(opts.ReadConnPool.MinOpenConns),
		crdb.ReadConnMaxIdleTime(opts.ReadConnPool.MaxIdleTime),
		crdb.ReadConnMaxLifetime(opts.ReadConnPool.MaxLifetime),
		crdb.ReadConnMaxLifetimeJitter(opts.ReadConnPool.MaxLifetimeJitter),
		crdb.ReadConnHealthCheckInterval(opts.ReadConnPool.HealthCheckInterval),
		crdb.WriteConnsMaxOpen(opts.WriteConnPool.MaxOpenConns),
		crdb.WriteConnsMinOpen(opts.WriteConnPool.MinOpenConns),
		crdb.WriteConnMaxIdleTime(opts.WriteConnPool.MaxIdleTime),
		crdb.WriteConnMaxLifetime(opts.WriteConnPool.MaxLifetime),
		crdb.WriteConnMaxLifetimeJitter(opts.WriteConnPool.MaxLifetimeJitter),
		crdb.WriteConnHealthCheckInterval(opts.WriteConnPool.HealthCheckInterval),
		crdb.FollowerReadDelay(opts.FollowerReadDelay),
		crdb.MaxRetries(maxRetries),
		crdb.OverlapKey(opts.OverlapKey),
		crdb.OverlapStrategy(opts.OverlapStrategy),
		crdb.WatchBufferLength(opts.WatchBufferLength),
		crdb.WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		crdb.WatchConnectTimeout(opts.WatchConnectTimeout),
		crdb.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		crdb.WithEnableConnectionBalancing(opts.EnableConnectionBalancing),
		crdb.ConnectRate(opts.ConnectRate),
		crdb.FilterMaximumIDCount(opts.FilterMaximumIDCount),
		crdb.WithIntegrity(opts.RelationshipIntegrityEnabled),
		crdb.AllowedMigrations(opts.AllowedMigrations),
		crdb.WithColumnOptimization(opts.ExperimentalColumnOptimization),
		crdb.IncludeQueryParametersInTraces(opts.IncludeQueryParametersInTraces),
		crdb.WithExpirationDisabled(!opts.EnableExperimentalRelationshipExpiration),
	)
}

func newPostgresDatastore(ctx context.Context, opts Config) (datastore.Datastore, error) {
	primary, err := newPostgresPrimaryDatastore(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary datastore: %w", err)
	}

	if len(opts.ReadReplicaURIs) > MaxReplicaCount {
		return nil, fmt.Errorf("too many read replicas, max is %d", MaxReplicaCount)
	}

	replicas := make([]datastore.StrictReadDatastore, 0, len(opts.ReadReplicaURIs))
	for index, replicaURI := range opts.ReadReplicaURIs {
		uintIndex, err := safecast.ToUint32(index)
		if err != nil {
			return nil, errors.New("too many replicas")
		}
		replica, err := newPostgresReplicaDatastore(ctx, uintIndex, replicaURI, opts)
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}

	return proxy.NewStrictReplicatedDatastore(primary, replicas...)
}

func commonPostgresDatastoreOptions(opts Config) ([]postgres.Option, error) {
	maxRetries, err := safecast.ToUint8(opts.MaxRetries)
	if err != nil {
		return nil, errors.New("max-retries could not be cast to uint8")
	}

	return []postgres.Option{
		postgres.EnableTracing(),
		postgres.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		postgres.MaxRetries(maxRetries),
		postgres.FilterMaximumIDCount(opts.FilterMaximumIDCount),
		postgres.WithColumnOptimization(opts.ExperimentalColumnOptimization),
		postgres.IncludeQueryParametersInTraces(opts.IncludeQueryParametersInTraces),
		postgres.WithExpirationDisabled(!opts.EnableExperimentalRelationshipExpiration),
	}, nil
}

func newPostgresReplicaDatastore(ctx context.Context, replicaIndex uint32, replicaURI string, opts Config) (datastore.StrictReadDatastore, error) {
	pgOpts := []postgres.Option{
		postgres.CredentialsProviderName(opts.ReadReplicaCredentialsProviderName),
		postgres.ReadConnsMaxOpen(opts.ReadReplicaConnPool.MaxOpenConns),
		postgres.ReadConnsMinOpen(opts.ReadReplicaConnPool.MinOpenConns),
		postgres.ReadConnMaxIdleTime(opts.ReadReplicaConnPool.MaxIdleTime),
		postgres.ReadConnMaxLifetime(opts.ReadReplicaConnPool.MaxLifetime),
		postgres.ReadConnMaxLifetimeJitter(opts.ReadReplicaConnPool.MaxLifetimeJitter),
		postgres.ReadConnHealthCheckInterval(opts.ReadReplicaConnPool.HealthCheckInterval),
		postgres.ReadStrictMode( /* strict read mode is required for Postgres read replicas */ true),
	}

	commonOptions, err := commonPostgresDatastoreOptions(opts)
	if err != nil {
		return nil, err
	}
	pgOpts = append(pgOpts, commonOptions...)
	return postgres.NewReadOnlyPostgresDatastore(ctx, replicaURI, replicaIndex, pgOpts...)
}

func newPostgresPrimaryDatastore(ctx context.Context, opts Config) (datastore.Datastore, error) {
	pgOpts := []postgres.Option{
		postgres.CredentialsProviderName(opts.CredentialsProviderName),
		postgres.GCWindow(opts.GCWindow),
		postgres.GCEnabled(!opts.ReadOnly),
		postgres.RevisionQuantization(opts.RevisionQuantization),
		postgres.MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		postgres.FollowerReadDelay(opts.FollowerReadDelay),
		postgres.ReadConnsMaxOpen(opts.ReadConnPool.MaxOpenConns),
		postgres.ReadConnsMinOpen(opts.ReadConnPool.MinOpenConns),
		postgres.ReadConnMaxIdleTime(opts.ReadConnPool.MaxIdleTime),
		postgres.ReadConnMaxLifetime(opts.ReadConnPool.MaxLifetime),
		postgres.ReadConnMaxLifetimeJitter(opts.ReadConnPool.MaxLifetimeJitter),
		postgres.ReadConnHealthCheckInterval(opts.ReadConnPool.HealthCheckInterval),
		postgres.WriteConnsMaxOpen(opts.WriteConnPool.MaxOpenConns),
		postgres.WriteConnsMinOpen(opts.WriteConnPool.MinOpenConns),
		postgres.WriteConnMaxIdleTime(opts.WriteConnPool.MaxIdleTime),
		postgres.WriteConnMaxLifetime(opts.WriteConnPool.MaxLifetime),
		postgres.WriteConnMaxLifetimeJitter(opts.ReadConnPool.MaxLifetimeJitter),
		postgres.WriteConnHealthCheckInterval(opts.WriteConnPool.HealthCheckInterval),
		postgres.GCInterval(opts.GCInterval),
		postgres.GCMaxOperationTime(opts.GCMaxOperationTime),
		postgres.WatchBufferLength(opts.WatchBufferLength),
		postgres.WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		postgres.MigrationPhase(opts.MigrationPhase),
		postgres.AllowedMigrations(opts.AllowedMigrations),
		postgres.WithRevisionHeartbeat(opts.EnableRevisionHeartbeat),
	}

	commonOptions, err := commonPostgresDatastoreOptions(opts)
	if err != nil {
		return nil, err
	}
	pgOpts = append(pgOpts, commonOptions...)
	return postgres.NewPostgresDatastore(ctx, opts.URI, pgOpts...)
}

func newSpannerDatastore(ctx context.Context, opts Config) (datastore.Datastore, error) {
	if len(opts.ReadReplicaURIs) > 0 {
		return nil, errors.New("read replicas are not supported for the Spanner datastore engine")
	}

	metricsOption := spanner.DatastoreMetricsOption(opts.SpannerDatastoreMetricsOption)
	if !opts.EnableDatastoreMetrics {
		metricsOption = spanner.DatastoreMetricsOptionNone
	}

	return spanner.NewSpannerDatastore(
		ctx,
		opts.URI,
		spanner.FollowerReadDelay(opts.FollowerReadDelay),
		spanner.RevisionQuantization(opts.RevisionQuantization),
		spanner.MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		spanner.CredentialsFile(opts.SpannerCredentialsFile),
		spanner.CredentialsJSON(opts.SpannerCredentialsJSON),
		spanner.WatchBufferLength(opts.WatchBufferLength),
		spanner.WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		spanner.EmulatorHost(opts.SpannerEmulatorHost),
		spanner.DisableStats(opts.DisableStats),
		spanner.WithDatastoreMetricsOption(metricsOption),
		spanner.ReadConnsMaxOpen(opts.ReadConnPool.MaxOpenConns),
		spanner.WriteConnsMaxOpen(opts.WriteConnPool.MaxOpenConns),
		spanner.MinSessionCount(opts.SpannerMinSessions),
		spanner.MaxSessionCount(opts.SpannerMaxSessions),
		spanner.MigrationPhase(opts.MigrationPhase),
		spanner.AllowedMigrations(opts.AllowedMigrations),
		spanner.FilterMaximumIDCount(opts.FilterMaximumIDCount),
		spanner.WithColumnOptimization(opts.ExperimentalColumnOptimization),
		spanner.WithExpirationDisabled(!opts.EnableExperimentalRelationshipExpiration),
	)
}

func newMySQLDatastore(ctx context.Context, opts Config) (datastore.Datastore, error) {
	primary, err := newMySQLPrimaryDatastore(ctx, opts)
	if err != nil {
		return nil, err
	}

	if len(opts.ReadReplicaURIs) > MaxReplicaCount {
		return nil, fmt.Errorf("too many read replicas, max is %d", MaxReplicaCount)
	}

	replicas := make([]datastore.ReadOnlyDatastore, 0, len(opts.ReadReplicaURIs))
	for index, replicaURI := range opts.ReadReplicaURIs {
		uintIndex, err := safecast.ToUint32(index)
		if err != nil {
			return nil, errors.New("too many replicas")
		}
		replica, err := newMySQLReplicaDatastore(ctx, uintIndex, replicaURI, opts)
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}

	return proxy.NewCheckingReplicatedDatastore(primary, replicas...)
}

func commonMySQLDatastoreOptions(opts Config) ([]mysql.Option, error) {
	maxRetries, err := safecast.ToUint8(opts.MaxRetries)
	if err != nil {
		return nil, errors.New("max-retries could not be cast to uint8")
	}

	return []mysql.Option{
		mysql.TablePrefix(opts.TablePrefix),
		mysql.MaxRetries(maxRetries),
		mysql.OverrideLockWaitTimeout(1),
		mysql.WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		mysql.MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		mysql.RevisionQuantization(opts.RevisionQuantization),
		mysql.FilterMaximumIDCount(opts.FilterMaximumIDCount),
		mysql.AllowedMigrations(opts.AllowedMigrations),
		mysql.WithColumnOptimization(opts.ExperimentalColumnOptimization),
		mysql.WithExpirationDisabled(!opts.EnableExperimentalRelationshipExpiration),
	}, nil
}

func newMySQLReplicaDatastore(ctx context.Context, replicaIndex uint32, replicaURI string, opts Config) (datastore.ReadOnlyDatastore, error) {
	mysqlOpts := []mysql.Option{
		mysql.MaxOpenConns(opts.ReadReplicaConnPool.MaxOpenConns),
		mysql.ConnMaxIdleTime(opts.ReadReplicaConnPool.MaxIdleTime),
		mysql.ConnMaxLifetime(opts.ReadReplicaConnPool.MaxLifetime),
		mysql.WatchBufferLength(opts.WatchBufferLength),
		mysql.WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		mysql.CredentialsProviderName(opts.ReadReplicaCredentialsProviderName),
	}

	commonOptions, err := commonMySQLDatastoreOptions(opts)
	if err != nil {
		return nil, err
	}
	mysqlOpts = append(mysqlOpts, commonOptions...)
	return mysql.NewReadOnlyMySQLDatastore(ctx, replicaURI, replicaIndex, mysqlOpts...)
}

func newMySQLPrimaryDatastore(ctx context.Context, opts Config) (datastore.Datastore, error) {
	mysqlOpts := []mysql.Option{
		mysql.GCInterval(opts.GCInterval),
		mysql.GCWindow(opts.GCWindow),
		mysql.GCInterval(opts.GCInterval),
		mysql.GCEnabled(!opts.ReadOnly),
		mysql.GCMaxOperationTime(opts.GCMaxOperationTime),
		mysql.MaxOpenConns(opts.ReadConnPool.MaxOpenConns),
		mysql.ConnMaxIdleTime(opts.ReadConnPool.MaxIdleTime),
		mysql.ConnMaxLifetime(opts.ReadConnPool.MaxLifetime),
		mysql.WatchBufferLength(opts.WatchBufferLength),
		mysql.WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		mysql.CredentialsProviderName(opts.CredentialsProviderName),
		mysql.FollowerReadDelay(opts.FollowerReadDelay),
	}

	commonOptions, err := commonMySQLDatastoreOptions(opts)
	if err != nil {
		return nil, err
	}
	mysqlOpts = append(mysqlOpts, commonOptions...)
	return mysql.NewMySQLDatastore(ctx, opts.URI, mysqlOpts...)
}

func newMemoryDatstore(_ context.Context, opts Config) (datastore.Datastore, error) {
	if len(opts.ReadReplicaURIs) > 0 {
		return nil, errors.New("read replicas are not supported for the in-memory datastore engine")
	}

	log.Warn().Msg("in-memory datastore is not persistent and not feasible to run in a high availability fashion")
	return memdb.NewMemdbDatastore(opts.WatchBufferLength, opts.RevisionQuantization, opts.GCWindow)
}
