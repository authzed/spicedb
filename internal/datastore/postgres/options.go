package postgres

import (
	"fmt"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
)

type postgresOptions struct {
	readPoolOpts, writePoolOpts pgxcommon.PoolOptions

	maxRevisionStalenessPercent float64

	credentialsProviderName string

	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration
	revisionQuantization    time.Duration
	followerReadDelay       time.Duration
	gcWindow                time.Duration
	gcInterval              time.Duration
	gcMaxOperationTime      time.Duration
	maxRetries              uint8
	filterMaximumIDCount    uint16

	enablePrometheusStats          bool
	analyzeBeforeStatistics        bool
	gcEnabled                      bool
	readStrictMode                 bool
	expirationDisabled             bool
	columnOptimizationOption       common.ColumnOptimizationOption
	includeQueryParametersInTraces bool
	revisionHeartbeatEnabled       bool

	migrationPhase    string
	allowedMigrations []string

	logger *tracingLogger

	queryInterceptor pgxcommon.QueryInterceptor
}

type migrationPhase uint8

const (
	writeBothReadOld migrationPhase = iota
	writeBothReadNew
	complete
)

var migrationPhases = map[string]migrationPhase{
	"write-both-read-old": writeBothReadOld,
	"write-both-read-new": writeBothReadNew,
	"":                    complete,
}

const (
	errQuantizationTooLarge = "revision quantization interval (%s) must be less than GC window (%s)"

	defaultWatchBufferLength                 = 128
	defaultWatchBufferWriteTimeout           = 1 * time.Second
	defaultGarbageCollectionWindow           = 24 * time.Hour
	defaultGarbageCollectionInterval         = time.Minute * 3
	defaultGarbageCollectionMaxOperationTime = time.Minute
	defaultQuantization                      = 5 * time.Second
	defaultMaxRevisionStalenessPercent       = 0.1
	defaultEnablePrometheusStats             = false
	defaultMaxRetries                        = 10
	defaultGCEnabled                         = true
	defaultCredentialsProviderName           = ""
	defaultReadStrictMode                    = false
	defaultFilterMaximumIDCount              = 100
	defaultColumnOptimizationOption          = common.ColumnOptimizationOptionNone
	defaultIncludeQueryParametersInTraces    = false
	defaultExpirationDisabled                = false
	// no follower delay by default, it should only be set if using read replicas
	defaultFollowerReadDelay = 0
	defaultRevisionHeartbeat = true
)

// Option provides the facility to configure how clients within the
// Postgres datastore interact with the running Postgres database.
type Option func(*postgresOptions)

func generateConfig(options []Option) (postgresOptions, error) {
	computed := postgresOptions{
		gcWindow:                       defaultGarbageCollectionWindow,
		gcInterval:                     defaultGarbageCollectionInterval,
		gcMaxOperationTime:             defaultGarbageCollectionMaxOperationTime,
		watchBufferLength:              defaultWatchBufferLength,
		watchBufferWriteTimeout:        defaultWatchBufferWriteTimeout,
		revisionQuantization:           defaultQuantization,
		maxRevisionStalenessPercent:    defaultMaxRevisionStalenessPercent,
		enablePrometheusStats:          defaultEnablePrometheusStats,
		maxRetries:                     defaultMaxRetries,
		gcEnabled:                      defaultGCEnabled,
		credentialsProviderName:        defaultCredentialsProviderName,
		readStrictMode:                 defaultReadStrictMode,
		queryInterceptor:               nil,
		filterMaximumIDCount:           defaultFilterMaximumIDCount,
		columnOptimizationOption:       defaultColumnOptimizationOption,
		includeQueryParametersInTraces: defaultIncludeQueryParametersInTraces,
		expirationDisabled:             defaultExpirationDisabled,
		followerReadDelay:              defaultFollowerReadDelay,
		revisionHeartbeatEnabled:       defaultRevisionHeartbeat,
	}

	for _, option := range options {
		option(&computed)
	}

	// Run any checks on the config that need to be done
	if computed.revisionQuantization >= computed.gcWindow {
		return computed, fmt.Errorf(
			errQuantizationTooLarge,
			computed.revisionQuantization,
			computed.gcWindow,
		)
	}

	if _, ok := migrationPhases[computed.migrationPhase]; !ok {
		return computed, fmt.Errorf("unknown migration phase: %s", computed.migrationPhase)
	}

	if computed.filterMaximumIDCount == 0 {
		computed.filterMaximumIDCount = 100
		log.Warn().Msg("filterMaximumIDCount not set, defaulting to 100")
	}

	return computed, nil
}

// ReadStrictMode sets whether strict mode is used for reads in the Postgres reader. If enabled,
// an assertion is added into the WHERE clause of all read queries to ensure that the revision
// being read is available on the read connection.
//
// Strict mode is disabled by default, as the default behavior is to read from the primary.
func ReadStrictMode(readStrictMode bool) Option {
	return func(po *postgresOptions) { po.readStrictMode = readStrictMode }
}

// ReadConnHealthCheckInterval is the frequency at which both idle and max
// lifetime connections are checked, and also the frequency at which the
// minimum number of connections is checked.
//
// This happens asynchronously.
//
// This is not the only approach to evaluate these counts; "connection idle/max
// lifetime" is also checked when connections are released to the pool.
//
// There is no guarantee connections won't last longer than their specified
// idle/max lifetime. It's largely dependent on the health-check goroutine
// being able to pull them from the connection pool.
//
// The health-check may not be able to clean up those connections if they are
// held by the application very frequently.
//
// This value defaults to 30s.
func ReadConnHealthCheckInterval(interval time.Duration) Option {
	return func(po *postgresOptions) { po.readPoolOpts.ConnHealthCheckInterval = &interval }
}

// WriteConnHealthCheckInterval is the frequency at which both idle and max
// lifetime connections are checked, and also the frequency at which the
// minimum number of connections is checked.
//
// This happens asynchronously.
//
// This is not the only approach to evaluate these counts; "connection idle/max
// lifetime" is also checked when connections are released to the pool.
//
// There is no guarantee connections won't last longer than their specified
// idle/max lifetime. It's largely dependent on the health-check goroutine
// being able to pull them from the connection pool.
//
// The health-check may not be able to clean up those connections if they are
// held by the application very frequently.
//
// This value defaults to 30s.
func WriteConnHealthCheckInterval(interval time.Duration) Option {
	return func(po *postgresOptions) { po.writePoolOpts.ConnHealthCheckInterval = &interval }
}

// ReadConnMaxIdleTime is the duration after which an idle read connection will
// be automatically closed by the health check.
//
// This value defaults to having no maximum.
func ReadConnMaxIdleTime(idle time.Duration) Option {
	return func(po *postgresOptions) { po.readPoolOpts.ConnMaxIdleTime = &idle }
}

// WriteConnMaxIdleTime is the duration after which an idle write connection
// will be automatically closed by the health check.
//
// This value defaults to having no maximum.
func WriteConnMaxIdleTime(idle time.Duration) Option {
	return func(po *postgresOptions) { po.writePoolOpts.ConnMaxIdleTime = &idle }
}

// ReadConnMaxLifetime is the duration since creation after which a read
// connection will be automatically closed.
//
// This value defaults to having no maximum.
func ReadConnMaxLifetime(lifetime time.Duration) Option {
	return func(po *postgresOptions) { po.readPoolOpts.ConnMaxLifetime = &lifetime }
}

// WriteConnMaxLifetime is the duration since creation after which a write
// connection will be automatically closed.
//
// This value defaults to having no maximum.
func WriteConnMaxLifetime(lifetime time.Duration) Option {
	return func(po *postgresOptions) { po.writePoolOpts.ConnMaxLifetime = &lifetime }
}

// ReadConnMaxLifetimeJitter is an interval to wait up to after the max lifetime
// to close the connection.
//
// This value defaults to 20% of the max lifetime.
func ReadConnMaxLifetimeJitter(jitter time.Duration) Option {
	return func(po *postgresOptions) { po.readPoolOpts.ConnMaxLifetimeJitter = &jitter }
}

// WriteConnMaxLifetimeJitter is an interval to wait up to after the max lifetime
// to close the connection.
//
// This value defaults to 20% of the max lifetime.
func WriteConnMaxLifetimeJitter(jitter time.Duration) Option {
	return func(po *postgresOptions) { po.writePoolOpts.ConnMaxLifetimeJitter = &jitter }
}

// ReadConnsMinOpen is the minimum size of the connection pool used for reads.
//
// The health check will increase the number of connections to this amount if
// it had dropped below.
//
// This value defaults to the maximum open connections.
func ReadConnsMinOpen(conns int) Option {
	return func(po *postgresOptions) { po.readPoolOpts.MinOpenConns = &conns }
}

// WriteConnsMinOpen is the minimum size of the connection pool used for writes.
//
// The health check will increase the number of connections to this amount if
// it had dropped below.
//
// This value defaults to the maximum open connections.
func WriteConnsMinOpen(conns int) Option {
	return func(po *postgresOptions) { po.writePoolOpts.MinOpenConns = &conns }
}

// ReadConnsMaxOpen is the maximum size of the connection pool used for reads.
//
// This value defaults to having no maximum.
func ReadConnsMaxOpen(conns int) Option {
	return func(po *postgresOptions) { po.readPoolOpts.MaxOpenConns = &conns }
}

// WriteConnsMaxOpen is the maximum size of the connection pool used for writes.
//
// This value defaults to having no maximum.
func WriteConnsMaxOpen(conns int) Option {
	return func(po *postgresOptions) { po.writePoolOpts.MaxOpenConns = &conns }
}

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(po *postgresOptions) { po.watchBufferLength = watchBufferLength }
}

// WatchBufferWriteTimeout is the maximum timeout for writing to the watch buffer,
// after which the caller to the watch will be disconnected.
func WatchBufferWriteTimeout(watchBufferWriteTimeout time.Duration) Option {
	return func(po *postgresOptions) { po.watchBufferWriteTimeout = watchBufferWriteTimeout }
}

// RevisionQuantization is the time bucket size to which advertised
// revisions will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(quantization time.Duration) Option {
	return func(po *postgresOptions) { po.revisionQuantization = quantization }
}

// FollowerReadDelay is the amount of time to round down the current time when
// reading from a read replica is expected.
//
// This value defaults to 0 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(po *postgresOptions) { po.followerReadDelay = delay }
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(po *postgresOptions) { po.maxRevisionStalenessPercent = stalenessPercent }
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 24 hours.
func GCWindow(window time.Duration) Option {
	return func(po *postgresOptions) { po.gcWindow = window }
}

// GCInterval is the the interval at which garbage collection will occur.
//
// This value defaults to 3 minutes.
func GCInterval(interval time.Duration) Option {
	return func(po *postgresOptions) { po.gcInterval = interval }
}

// GCMaxOperationTime is the maximum operation time of a garbage collection
// pass before it times out.
//
// This value defaults to 1 minute.
func GCMaxOperationTime(time time.Duration) Option {
	return func(po *postgresOptions) { po.gcMaxOperationTime = time }
}

// MaxRetries is the maximum number of times a retriable transaction will be
// client-side retried.
// Default: 10
func MaxRetries(maxRetries uint8) Option {
	return func(po *postgresOptions) { po.maxRetries = maxRetries }
}

// WithEnablePrometheusStats marks whether Prometheus metrics provided by the Postgres
// clients being used by the datastore are enabled.
//
// Prometheus metrics are disabled by default.
func WithEnablePrometheusStats(enablePrometheusStats bool) Option {
	return func(po *postgresOptions) { po.enablePrometheusStats = enablePrometheusStats }
}

// EnableTracing enables trace-level logging for the Postgres clients being
// used by the datastore.
//
// Tracing is disabled by default.
func EnableTracing() Option {
	return func(po *postgresOptions) { po.logger = &tracingLogger{} }
}

// GCEnabled indicates whether garbage collection is enabled.
//
// GC is enabled by default.
func GCEnabled(isGCEnabled bool) Option {
	return func(po *postgresOptions) { po.gcEnabled = isGCEnabled }
}

// DebugAnalyzeBeforeStatistics signals to the Statistics method that it should
// run Analyze on the database before returning statistics. This should only be
// used for debug and testing.
//
// Disabled by default.
func DebugAnalyzeBeforeStatistics() Option {
	return func(po *postgresOptions) { po.analyzeBeforeStatistics = true }
}

// WithQueryInterceptor adds an interceptor to all underlying postgres queries
//
// By default, no query interceptor is used.
func WithQueryInterceptor(interceptor pgxcommon.QueryInterceptor) Option {
	return func(po *postgresOptions) {
		po.queryInterceptor = interceptor
	}
}

// MigrationPhase configures the postgres driver to the proper state of a
// multi-phase migration.
//
// Steady-state configuration (e.g. fully migrated) by default
func MigrationPhase(phase string) Option {
	return func(po *postgresOptions) { po.migrationPhase = phase }
}

// AllowedMigrations configures a set of additional migrations that will pass
// the health check (head migration is always allowed).
func AllowedMigrations(allowedMigrations []string) Option {
	return func(po *postgresOptions) { po.allowedMigrations = allowedMigrations }
}

// CredentialsProviderName is the name of the CredentialsProvider implementation to use
// for dynamically retrieving the datastore credentials at runtime
//
// Empty by default.
func CredentialsProviderName(credentialsProviderName string) Option {
	return func(po *postgresOptions) { po.credentialsProviderName = credentialsProviderName }
}

// FilterMaximumIDCount is the maximum number of IDs that can be used to filter IDs in queries
func FilterMaximumIDCount(filterMaximumIDCount uint16) Option {
	return func(po *postgresOptions) { po.filterMaximumIDCount = filterMaximumIDCount }
}

// IncludeQueryParametersInTraces is a flag to set whether to include query parameters in OTEL traces
func IncludeQueryParametersInTraces(includeQueryParametersInTraces bool) Option {
	return func(po *postgresOptions) { po.includeQueryParametersInTraces = includeQueryParametersInTraces }
}

// WithColumnOptimization sets the column optimization option for the datastore.
func WithColumnOptimization(isEnabled bool) Option {
	return func(po *postgresOptions) {
		if isEnabled {
			po.columnOptimizationOption = common.ColumnOptimizationOptionStaticValues
		} else {
			po.columnOptimizationOption = common.ColumnOptimizationOptionNone
		}
	}
}

// WithExpirationDisabled disables support for relationship expiration.
func WithExpirationDisabled(isDisabled bool) Option {
	return func(po *postgresOptions) { po.expirationDisabled = isDisabled }
}

// WithRevisionHeartbeat enables the revision heartbeat.
func WithRevisionHeartbeat(isEnabled bool) Option {
	return func(po *postgresOptions) { po.revisionHeartbeatEnabled = isEnabled }
}
