package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/internal/datastore/proxy"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func RegisterConnPoolFlagsWithPrefix(flagSet *pflag.FlagSet, prefix string, defaults, opts *ConnPoolConfig) {
	if prefix != "" {
		prefix += "-"
	}
	flagName := func(flag string) string {
		return prefix + flag
	}

	flagSet.IntVar(&opts.MaxOpenConns, flagName("max-open"), defaults.MaxOpenConns, "number of concurrent connections open in a remote datastore's connection pool")
	flagSet.IntVar(&opts.MinOpenConns, flagName("min-open"), defaults.MinOpenConns, "number of minimum concurrent connections open in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.MaxLifetime, flagName("max-lifetime"), defaults.MaxLifetime, "maximum amount of time a connection can live in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.MaxLifetimeJitter, flagName("max-lifetime-jitter"), defaults.MaxLifetimeJitter, "waits rand(0, jitter) after a connection is open for max lifetime to actually close the connection (default: 20% of max lifetime, 30m for CockroachDB)")
	flagSet.DurationVar(&opts.MaxIdleTime, flagName("max-idletime"), defaults.MaxIdleTime, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.HealthCheckInterval, flagName("healthcheck-interval"), defaults.HealthCheckInterval, "amount of time between connection health checks in a remote datastore's connection pool")
	flagSet.DurationVar(&opts.PingTimeout, flagName("ping-timeout"), defaults.PingTimeout, "amount of time to wait for a liveness ping against an acquired idle connection before discarding it in a remote datastore's connection pool")
}

func deprecateUnifiedConnFlags(flagSet *pflag.FlagSet) {
	const warning = "connection pooling has been split into read and write pools"
	_ = flagSet.MarkDeprecated("datastore-conn-max-open", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-min-open", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-max-lifetime", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-max-idletime", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-healthcheck-interval", warning)
	_ = flagSet.MarkDeprecated("datastore-conn-ping-timeout", warning)
}

// RegisterDatastoreFlags adds datastore flags to a cobra command.
func RegisterDatastoreFlags(flagset *pflag.FlagSet, opts *Config) error {
	return RegisterDatastoreFlagsWithPrefix(flagset, "", opts)
}

// RegisterDatastoreFlagsWithPrefix adds datastore flags to a cobra command, with each flag prefixed with the provided
// prefix argument. If left empty, the datastore flags are not prefixed.
func RegisterDatastoreFlagsWithPrefix(flagSet *pflag.FlagSet, prefix string, opts *Config) error {
	if prefix != "" {
		prefix += "-"
	}
	flagName := func(flag string) string {
		return prefix + flag
	}
	defaults := DefaultDatastoreConfig()

	// NOTE: we set this manually here because this is a value that was never intended to be
	// controlled by an external flag, but we still want the default value to be propagated through.
	opts.CaveatTypeSet = defaults.CaveatTypeSet

	flagSet.StringVar(&opts.Engine, flagName("datastore-engine"), defaults.Engine, fmt.Sprintf(`type of datastore to initialize (%s)`, datastore.EngineOptions()))
	flagSet.StringVar(&opts.URI, flagName("datastore-conn-uri"), defaults.URI, `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	flagSet.StringVar(&opts.CredentialsProviderName, flagName("datastore-credentials-provider-name"), defaults.CredentialsProviderName, fmt.Sprintf(`retrieve datastore credentials dynamically using (%s)`, datastore.CredentialsProviderOptions()))

	flagSet.StringArrayVar(&opts.ReadReplicaURIs, flagName("datastore-read-replica-conn-uri"), []string{}, "connection string used by remote datastores for read replicas (e.g. \"postgres://postgres:password@localhost:5432/spicedb\"). (Postgres and MySQL drivers only).")
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

	// ping-timeout is an internal pool-tuning knob; keep it hidden from --help
	// until load testing settles a good default. The 5s default still applies.
	for _, prefix := range []string{"datastore-conn-pool-read", "datastore-conn-pool-write", newReadReplicaPrefix} {
		if err := flagSet.MarkHidden(prefix + "-ping-timeout"); err != nil {
			return fmt.Errorf("failed to mark flag as hidden: %w", err)
		}
	}

	warning := fmt.Sprintf("please use the flags with the prefix %q instead of %q", newReadReplicaPrefix, oldReadReplicaPrefix)
	for _, flag := range []string{"max-open", "min-open", "max-lifetime", "max-lifetime-jitter", "max-idletime", "healthcheck-interval", "ping-timeout"} {
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

	flagSet.DurationVar(&opts.GCWindow, flagName("datastore-gc-window"), defaults.GCWindow, "how far into the past clients may read: revisions older than this are rejected as stale, regardless of whether their data has been physically deleted yet")
	flagSet.DurationVar(&opts.GCInterval, flagName("datastore-gc-interval"), defaults.GCInterval, "how often the background worker deletes data that has aged out of the gc window; affects disk usage only, never which revisions are readable (Postgres and MySQL only)")
	flagSet.DurationVar(&opts.GCMaxOperationTime, flagName("datastore-gc-max-operation-time"), defaults.GCMaxOperationTime, "maximum amount of time a garbage collection pass can operate before timing out (Postgres and MySQL only)")
	flagSet.DurationVar(&opts.RevisionQuantization, flagName("datastore-revision-quantization-interval"), defaults.RevisionQuantization, "boundary interval to which to round the quantized revision")
	flagSet.Float64Var(&opts.MaxRevisionStalenessPercent, flagName("datastore-revision-quantization-max-staleness-percent"), defaults.MaxRevisionStalenessPercent, "float percentage (where 1 = 100%) of the revision quantization interval where we may opt to select a stale revision for performance reasons. Defaults to 0.1 (representing 10%)")
	flagSet.BoolVar(&opts.ReadOnly, flagName("datastore-readonly"), defaults.ReadOnly, "set the service to read-only mode")
	flagSet.StringSliceVar(&opts.BootstrapFiles, flagName("datastore-bootstrap-files"), defaults.BootstrapFiles, "bootstrap data yaml files to load")
	flagSet.BoolVar(&opts.BootstrapOverwrite, flagName("datastore-bootstrap-overwrite"), defaults.BootstrapOverwrite, "overwrite any existing data with bootstrap data (this can be quite slow)")
	flagSet.DurationVar(&opts.BootstrapTimeout, flagName("datastore-bootstrap-timeout"), defaults.BootstrapTimeout, "maximum duration before timeout for the bootstrap data to be written")

	flagSet.BoolVar(&opts.RequestHedgingEnabled, flagName("datastore-request-hedging"), defaults.RequestHedgingEnabled, "enable request hedging")
	err := flagSet.MarkDeprecated(flagName("datastore-request-hedging"), "hedging functionality has been removed and this flag is now a no-op")
	if err != nil {
		return err
	}
	flagSet.DurationVar(&opts.RequestHedgingInitialSlowValue, flagName("datastore-request-hedging-initial-slow-value"), defaults.RequestHedgingInitialSlowValue, "initial value to use for slow datastore requests, before statistics have been collected")
	err = flagSet.MarkDeprecated(flagName("datastore-request-hedging-initial-slow-value"), "hedging functionality has been removed and this flag is now a no-op")
	if err != nil {
		return err
	}
	flagSet.Uint64Var(&opts.RequestHedgingMaxRequests, flagName("datastore-request-hedging-max-requests"), defaults.RequestHedgingMaxRequests, "maximum number of historical requests to consider")
	err = flagSet.MarkDeprecated(flagName("datastore-request-hedging-max-requests"), "hedging functionality has been removed and this flag is now a no-op")
	if err != nil {
		return err
	}
	flagSet.Float64Var(&opts.RequestHedgingQuantile, flagName("datastore-request-hedging-quantile"), defaults.RequestHedgingQuantile, "quantile of historical datastore request time over which a request will be considered slow")
	err = flagSet.MarkDeprecated(flagName("datastore-request-hedging-quantile"), "hedging functionality has been removed and this flag is now a no-op")
	if err != nil {
		return err
	}

	flagSet.BoolVar(&opts.EnableDatastoreMetrics, flagName("datastore-prometheus-metrics"), defaults.EnableDatastoreMetrics, "set to false to disable metrics from the datastore (do not use for Spanner; setting to false will disable metrics to the configured metrics store in Spanner)")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	flagSet.DurationVar(&opts.FollowerReadDelay, flagName("datastore-follower-read-delay-duration"), DefaultFollowerReadDelay, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (CockroachDB and Spanner drivers only) or read replicas (Postgres and MySQL drivers only)")
	flagSet.IntVar(&opts.MaxRetries, flagName("datastore-max-tx-retries"), 10, "number of times a retriable transaction should be retried")
	flagSet.StringVar(&opts.OverlapStrategy, flagName("datastore-tx-overlap-strategy"), "static", "strategy to generate transaction overlap keys (\"request\", \"prefix\", \"static\", \"insecure\") (CockroachDB driver only - see "+sharederrors.CrdbOverlapErrorLink+" for details)")
	flagSet.StringVar(&opts.OverlapKey, flagName("datastore-tx-overlap-key"), "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; CockroachDB driver only)")
	flagSet.BoolVar(&opts.EnableConnectionBalancing, flagName("datastore-connection-balancing"), defaults.EnableConnectionBalancing, "enable connection balancing between database nodes (CockroachDB driver only)")
	flagSet.DurationVar(&opts.ConnectRate, flagName("datastore-connect-rate"), 100*time.Millisecond, "rate at which new connections are allowed to the datastore (at a rate of 1/duration) (CockroachDB driver only)")
	//nolint:staticcheck // the deprecated flag remains supported until removal
	flagSet.StringVar(&opts.SpannerCredentialsFile, flagName("datastore-spanner-credentials"), "", "path to service account key credentials file with access to the cloud spanner instance (omit to use application default credentials)")
	err = flagSet.MarkDeprecated(flagName("datastore-spanner-credentials"), "prefer Application Default Credentials: https://docs.cloud.google.com/docs/authentication/client-libraries#adc")
	if err != nil {
		return err
	}
	flagSet.StringVar(&opts.SpannerEmulatorHost, flagName("datastore-spanner-emulator-host"), "", "URI of spanner emulator instance used for development and testing (e.g. localhost:9010)")
	flagSet.Uint64Var(&opts.SpannerMinSessions, flagName("datastore-spanner-min-sessions"), 100, "minimum number of sessions across all Spanner gRPC connections the client can have at a given time")
	err = flagSet.MarkDeprecated(flagName("datastore-spanner-min-sessions"), "sessions flags are deprecated as the client no longer uses a session pool")
	if err != nil {
		return err
	}
	flagSet.Uint64Var(&opts.SpannerMaxSessions, flagName("datastore-spanner-max-sessions"), 400, "maximum number of sessions across all Spanner gRPC connections the client can have at a given time")
	err = flagSet.MarkDeprecated(flagName("datastore-spanner-max-sessions"), "sessions flags are deprecated as the client no longer uses a session pool")
	if err != nil {
		return err
	}
	flagSet.StringVar(&opts.SpannerDatastoreMetricsOption, flagName("datastore-spanner-metrics"), "otel", `configure the metrics that are emitted by the Spanner datastore ("none", "native", "otel")`)
	flagSet.StringVar(&opts.TablePrefix, flagName("datastore-mysql-table-prefix"), "", "prefix to add to the name of all SpiceDB database tables")
	flagSet.StringVar(&opts.MigrationPhase, flagName("datastore-migration-phase"), "", "datastore-specific flag that should be used to signal to a datastore which phase of a multi-step migration it is in")
	flagSet.StringArrayVar(&opts.AllowedMigrations, flagName("datastore-allowed-migrations"), []string{}, "migration levels that will not fail the health check (in addition to the current head migration)")
	flagSet.Uint16Var(&opts.WatchBufferLength, flagName("datastore-watch-buffer-length"), 1024, "how large the watch buffer should be before blocking")
	flagSet.StringVar(&opts.WatchChangeBufferMaximumSize, flagName("datastore-watch-change-buffer-maximum-size"), "15%", "how much memory to reserve for the watch change buffer, either as a quantity of bytes (e.g. 5Gi) or a percentage of available memory (e.g. 50%). if this value is exceeded, the watch will error and must be restarted.")
	flagSet.DurationVar(&opts.WatchBufferWriteTimeout, flagName("datastore-watch-buffer-write-timeout"), 1*time.Second, "how long the watch buffer should queue before forcefully disconnecting the reader")
	flagSet.DurationVar(&opts.WatchConnectTimeout, flagName("datastore-watch-connect-timeout"), 1*time.Second, "how long the watch connection to the underlying datastore should wait before timing out (CockroachDB driver only)")
	flagSet.BoolVar(&opts.DisableWatchSupport, flagName("datastore-disable-watch-support"), false, "disable watch support (only enable if you absolutely do not need watch)")
	flagSet.BoolVar(&opts.IncludeQueryParametersInTraces, flagName("datastore-include-query-parameters-in-traces"), false, "include query parameters in traces (Postgres and CockroachDB drivers only)")
	flagSet.DurationVar(&opts.WriteAcquisitionTimeout, flagName("write-conn-acquisition-timeout"), defaults.WriteAcquisitionTimeout, "amount of time that the server will wait for a connection to the datastore to become available when performing a write operation before throwing a ResourceExhausted error. 0 means wait indefinitely. (CockroachDB driver only)")

	flagSet.BoolVar(&opts.RelationshipIntegrityEnabled, flagName("datastore-relationship-integrity-enabled"), false, "enables relationship integrity checks. (CockroachDB driver only)")
	flagSet.StringVar(&opts.RelationshipIntegrityCurrentKey.KeyID, flagName("datastore-relationship-integrity-current-key-id"), "", "current key id for relationship integrity checks")
	flagSet.StringVar(&opts.RelationshipIntegrityCurrentKey.KeyFilename, flagName("datastore-relationship-integrity-current-key-filename"), "", "current key filename for relationship integrity checks")
	flagSet.StringArrayVar(&opts.RelationshipIntegrityExpiredKeys, flagName("datastore-relationship-integrity-expired-keys"), []string{}, "config for expired keys for relationship integrity checks")

	// disabling stats is only for tests
	flagSet.BoolVar(&opts.DisableStats, flagName("datastore-disable-stats"), false, "disable recording relationship counts to the stats table")
	if err := flagSet.MarkHidden(flagName("datastore-disable-stats")); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	flagSet.BoolVar(&opts.RelaxedIsolationLevel, flagName("datastore-relaxed-isolation-level"), false, "used to relax the isolation level used in transactions (Postgres driver only)")
	if err := flagSet.MarkHidden(flagName("datastore-relaxed-isolation-level")); err != nil {
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

	flagSet.BoolVar(&opts.ExperimentalColumnOptimization, flagName("datastore-experimental-column-optimization"), true, "enable experimental column optimization")

	return nil
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

		nsDefs, err := ds.SnapshotReader(revision.Revision).LegacyListAllNamespaces(ctx)
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

		// Combine bootstrap files and direct contents into a single set so that
		// all definitions are written together (WriteSchema replaces the full schema).
		bootstrapContents := make(map[string][]byte, len(opts.BootstrapFiles)+len(opts.BootstrapFileContents))
		for _, filePath := range opts.BootstrapFiles {
			fileContents, rerr := os.ReadFile(filePath)
			if rerr != nil {
				return nil, fmt.Errorf("failed to read bootstrap file %s: %w", filePath, rerr)
			}
			bootstrapContents[filePath] = fileContents
		}
		for k, v := range opts.BootstrapFileContents {
			bootstrapContents[k] = v
		}

		if len(bootstrapContents) > 0 {
			bootstrapDL := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(opts.BootstrapSchemaMode))
			_, _, err = validationfile.PopulateFromFilesContents(ctx, bootstrapDL, opts.CaveatTypeSet, bootstrapContents)
			if err != nil {
				return nil, fmt.Errorf("failed to load bootstrap data: %w", err)
			}
		}
		log.Ctx(ctx).Info().Strs("files", opts.BootstrapFiles).Msg("completed datastore initialization from bootstrap files")
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
