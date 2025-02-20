package mysql

import (
	"fmt"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
)

const (
	errQuantizationTooLarge = "revision quantization interval (%s) must be less than GC window (%s)"

	defaultGarbageCollectionWindow           = 24 * time.Hour
	defaultGarbageCollectionInterval         = time.Minute * 3
	defaultGarbageCollectionMaxOperationTime = time.Minute
	defaultMaxOpenConns                      = 20
	defaultConnMaxIdleTime                   = 30 * time.Minute
	defaultConnMaxLifetime                   = 30 * time.Minute
	defaultWatchBufferLength                 = 128
	defaultWatchBufferWriteTimeout           = 1 * time.Second
	defaultQuantization                      = 5 * time.Second
	defaultMaxRevisionStalenessPercent       = 0.1
	defaultEnablePrometheusStats             = false
	defaultMaxRetries                        = 8
	defaultGCEnabled                         = true
	defaultCredentialsProviderName           = ""
	defaultFilterMaximumIDCount              = 100
	defaultColumnOptimizationOption          = common.ColumnOptimizationOptionNone
	defaultExpirationDisabled                = false
	// no follower delay by default, it should only be set if using read replicas
	defaultFollowerReadDelay = 0
)

type mysqlOptions struct {
	revisionQuantization        time.Duration
	gcWindow                    time.Duration
	gcInterval                  time.Duration
	gcMaxOperationTime          time.Duration
	maxRevisionStalenessPercent float64
	followerReadDelay           time.Duration
	watchBufferLength           uint16
	watchBufferWriteTimeout     time.Duration
	tablePrefix                 string
	enablePrometheusStats       bool
	maxOpenConns                int
	connMaxIdleTime             time.Duration
	connMaxLifetime             time.Duration
	analyzeBeforeStats          bool
	maxRetries                  uint8
	lockWaitTimeoutSeconds      *uint8
	gcEnabled                   bool
	credentialsProviderName     string
	filterMaximumIDCount        uint16
	allowedMigrations           []string
	columnOptimizationOption    common.ColumnOptimizationOption
	expirationDisabled          bool
}

// Option provides the facility to configure how clients within the
// MySQL datastore interact with the running MySQL database.
type Option func(*mysqlOptions)

func generateConfig(options []Option) (mysqlOptions, error) {
	computed := mysqlOptions{
		gcWindow:                    defaultGarbageCollectionWindow,
		gcInterval:                  defaultGarbageCollectionInterval,
		gcMaxOperationTime:          defaultGarbageCollectionMaxOperationTime,
		watchBufferLength:           defaultWatchBufferLength,
		watchBufferWriteTimeout:     defaultWatchBufferWriteTimeout,
		maxOpenConns:                defaultMaxOpenConns,
		connMaxIdleTime:             defaultConnMaxIdleTime,
		connMaxLifetime:             defaultConnMaxLifetime,
		revisionQuantization:        defaultQuantization,
		maxRevisionStalenessPercent: defaultMaxRevisionStalenessPercent,
		enablePrometheusStats:       defaultEnablePrometheusStats,
		maxRetries:                  defaultMaxRetries,
		gcEnabled:                   defaultGCEnabled,
		credentialsProviderName:     defaultCredentialsProviderName,
		filterMaximumIDCount:        defaultFilterMaximumIDCount,
		columnOptimizationOption:    defaultColumnOptimizationOption,
		expirationDisabled:          defaultExpirationDisabled,
		followerReadDelay:           defaultFollowerReadDelay,
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

	if computed.filterMaximumIDCount == 0 {
		computed.filterMaximumIDCount = 100
		log.Warn().Msg("filterMaximumIDCount not set, defaulting to 100")
	}

	return computed, nil
}

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(mo *mysqlOptions) {
		mo.watchBufferLength = watchBufferLength
	}
}

// WatchBufferWriteTimeout is the maximum timeout for writing to the watch buffer,
// after which the caller to the watch will be disconnected.
func WatchBufferWriteTimeout(watchBufferWriteTimeout time.Duration) Option {
	return func(mo *mysqlOptions) { mo.watchBufferWriteTimeout = watchBufferWriteTimeout }
}

// RevisionQuantization is the time bucket size to which advertised
// revisions will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(quantization time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.revisionQuantization = quantization
	}
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(mo *mysqlOptions) {
		mo.maxRevisionStalenessPercent = stalenessPercent
	}
}

// FollowerReadDelay is the amount of time to round down the current time when
// reading from a read replica is expected.
//
// This value defaults to 0 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(mo *mysqlOptions) { mo.followerReadDelay = delay }
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 24 hours.
func GCWindow(window time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.gcWindow = window
	}
}

// GCInterval is the interval at which garbage collection will occur.
//
// This value defaults to 3 minutes.
func GCInterval(interval time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.gcInterval = interval
	}
}

// MaxRetries is the maximum number of times a retriable transaction will be
// client-side retried.
//
// Default: 10
func MaxRetries(maxRetries uint8) Option {
	return func(mo *mysqlOptions) {
		mo.maxRetries = maxRetries
	}
}

// TablePrefix allows defining a MySQL table name prefix.
//
// No prefix is set by default
func TablePrefix(prefix string) Option {
	return func(mo *mysqlOptions) {
		mo.tablePrefix = prefix
	}
}

// WithEnablePrometheusStats marks whether Prometheus metrics provided by Go's database/sql package
// are enabled.
//
// Prometheus metrics are disabled by default.
func WithEnablePrometheusStats(enablePrometheusStats bool) Option {
	return func(mo *mysqlOptions) {
		mo.enablePrometheusStats = enablePrometheusStats
	}
}

// ConnMaxIdleTime is the duration after which an idle connection will be
// automatically closed.
// See https://pkg.go.dev/database/sql#DB.SetConnMaxIdleTime/
//
// This value defaults to having no maximum.
func ConnMaxIdleTime(idle time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.connMaxIdleTime = idle
	}
}

// ConnMaxLifetime is the duration since creation after which a connection will
// be automatically closed.
// See https://pkg.go.dev/database/sql#DB.SetConnMaxLifetime
//
// This value defaults to having no maximum.
func ConnMaxLifetime(lifetime time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.connMaxLifetime = lifetime
	}
}

// MaxOpenConns is the maximum size of the connection pool.
// See https://pkg.go.dev/database/sql#DB.SetMaxOpenConns
//
// This value defaults to having no maximum.
func MaxOpenConns(conns int) Option {
	return func(mo *mysqlOptions) {
		mo.maxOpenConns = conns
	}
}

// DebugAnalyzeBeforeStatistics signals to the Statistics method that it should
// run Analyze Table on the relationships table before returning statistics.
// This should only be used for debug and testing.
//
// Disabled by default.
func DebugAnalyzeBeforeStatistics() Option {
	return func(mo *mysqlOptions) {
		mo.analyzeBeforeStats = true
	}
}

// OverrideLockWaitTimeout sets the lock wait timeout on each new connection established
// with the databases. As an OLTP service, the default of 50s is unbearably long to block
// a write for our service, so we suggest setting this value to the minimum of 1 second.
//
// Uses server default by default.
func OverrideLockWaitTimeout(seconds uint8) Option {
	return func(mo *mysqlOptions) {
		mo.lockWaitTimeoutSeconds = &seconds
	}
}

// GCEnabled indicates whether garbage collection is enabled.
//
// GC is enabled by default.
func GCEnabled(isGCEnabled bool) Option {
	return func(mo *mysqlOptions) {
		mo.gcEnabled = isGCEnabled
	}
}

// GCMaxOperationTime is the maximum operation time of a garbage collection
// pass before it times out.
//
// This value defaults to 1 minute.
func GCMaxOperationTime(time time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.gcMaxOperationTime = time
	}
}

// CredentialsProviderName is the name of the CredentialsProvider implementation to use
// for dynamically retrieving the datastore credentials at runtime
//
// Empty by default.
func CredentialsProviderName(credentialsProviderName string) Option {
	return func(mo *mysqlOptions) { mo.credentialsProviderName = credentialsProviderName }
}

// FilterMaximumIDCount is the maximum number of IDs that can be used to filter IDs in queries
func FilterMaximumIDCount(filterMaximumIDCount uint16) Option {
	return func(mo *mysqlOptions) { mo.filterMaximumIDCount = filterMaximumIDCount }
}

// AllowedMigrations configures a set of additional migrations that will pass
// the health check (head migration is always allowed).
func AllowedMigrations(allowedMigrations []string) Option {
	return func(mo *mysqlOptions) { mo.allowedMigrations = allowedMigrations }
}

// WithColumnOptimization configures the column optimization strategy for the MySQL datastore.
func WithColumnOptimization(isEnabled bool) Option {
	return func(mo *mysqlOptions) {
		if isEnabled {
			mo.columnOptimizationOption = common.ColumnOptimizationOptionStaticValues
		} else {
			mo.columnOptimizationOption = common.ColumnOptimizationOptionNone
		}
	}
}

// WithExpirationDisabled disables the expiration of relationships in the MySQL datastore.
func WithExpirationDisabled(isDisabled bool) Option {
	return func(mo *mysqlOptions) {
		mo.expirationDisabled = isDisabled
	}
}
