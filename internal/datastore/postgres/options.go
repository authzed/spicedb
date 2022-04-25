package postgres

import (
	"fmt"
	"time"
)

type postgresOptions struct {
	connMaxIdleTime             *time.Duration
	connMaxLifetime             *time.Duration
	healthCheckPeriod           *time.Duration
	maxOpenConns                *int
	minOpenConns                *int
	maxRevisionStalenessPercent float64

	watchBufferLength    uint16
	revisionQuantization time.Duration
	gcWindow             time.Duration
	gcInterval           time.Duration
	gcMaxOperationTime   time.Duration
	splitAtUsersetCount  uint16

	enablePrometheusStats   bool
	analyzeBeforeStatistics bool

	logger *tracingLogger
}

const (
	errQuantizationTooLarge = "revision quantization interval (%s) must be less than GC window (%s)"

	defaultWatchBufferLength                 = 128
	defaultGarbageCollectionWindow           = 24 * time.Hour
	defaultGarbageCollectionInterval         = time.Minute * 3
	defaultGarbageCollectionMaxOperationTime = time.Minute
	defaultUsersetBatchSize                  = 1024
	defaultQuantization                      = 5 * time.Second
	defaultMaxRevisionStalenessPercent       = 0.1
)

// Option provides the facility to configure how clients within the
// Postgres datastore interact with the running Postgres database.
type Option func(*postgresOptions)

func generateConfig(options []Option) (postgresOptions, error) {
	computed := postgresOptions{
		gcWindow:                    defaultGarbageCollectionWindow,
		gcInterval:                  defaultGarbageCollectionInterval,
		gcMaxOperationTime:          defaultGarbageCollectionMaxOperationTime,
		watchBufferLength:           defaultWatchBufferLength,
		splitAtUsersetCount:         defaultUsersetBatchSize,
		revisionQuantization:        defaultQuantization,
		maxRevisionStalenessPercent: defaultMaxRevisionStalenessPercent,
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

	return computed, nil
}

// SplitAtUsersetCount is the batch size for which userset queries will be
// split into smaller queries.
//
// This defaults to 1024.
func SplitAtUsersetCount(splitAtUsersetCount uint16) Option {
	return func(po *postgresOptions) {
		po.splitAtUsersetCount = splitAtUsersetCount
	}
}

// ConnMaxIdleTime is the duration after which an idle connection will be
// automatically closed by the health check.
//
// This value defaults to having no maximum.
func ConnMaxIdleTime(idle time.Duration) Option {
	return func(po *postgresOptions) {
		po.connMaxIdleTime = &idle
	}
}

// ConnMaxLifetime is the duration since creation after which a connection will
// be automatically closed.
//
// This value defaults to having no maximum.
func ConnMaxLifetime(lifetime time.Duration) Option {
	return func(po *postgresOptions) {
		po.connMaxLifetime = &lifetime
	}
}

// HealthCheckPeriod is the interval by which idle Postgres client connections
// are health checked in order to keep them alive in a connection pool.
func HealthCheckPeriod(period time.Duration) Option {
	return func(po *postgresOptions) {
		po.healthCheckPeriod = &period
	}
}

// MaxOpenConns is the maximum size of the connection pool.
//
// This value defaults to having no maximum.
func MaxOpenConns(conns int) Option {
	return func(po *postgresOptions) {
		po.maxOpenConns = &conns
	}
}

// MinOpenConns is the minimum size of the connection pool.
// The health check will increase the number of connections to this amount if
// it had dropped below.
//
// This value defaults to zero.
func MinOpenConns(conns int) Option {
	return func(po *postgresOptions) {
		po.minOpenConns = &conns
	}
}

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(po *postgresOptions) {
		po.watchBufferLength = watchBufferLength
	}
}

// RevisionQuantization is the time bucket size to which advertised
// revisions will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(quantization time.Duration) Option {
	return func(po *postgresOptions) {
		po.revisionQuantization = quantization
	}
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(po *postgresOptions) {
		po.maxRevisionStalenessPercent = stalenessPercent
	}
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 24 hours.
func GCWindow(window time.Duration) Option {
	return func(po *postgresOptions) {
		po.gcWindow = window
	}
}

// GCInterval is the the interval at which garbage collection will occur.
//
// This value defaults to 3 minutes.
func GCInterval(interval time.Duration) Option {
	return func(po *postgresOptions) {
		po.gcInterval = interval
	}
}

// GCMaxOperationTime is the maximum operation time of a garbage collection
// pass before it times out.
//
// This value defaults to 1 minute.
func GCMaxOperationTime(time time.Duration) Option {
	return func(po *postgresOptions) {
		po.gcMaxOperationTime = time
	}
}

// EnablePrometheusStats enables Prometheus metrics provided by the Postgres
// clients being used by the datastore.
//
// Prometheus metrics are disable by default.
func EnablePrometheusStats() Option {
	return func(po *postgresOptions) {
		po.enablePrometheusStats = true
	}
}

// EnableTracing enables trace-level logging for the Postgres clients being
// used by the datastore.
//
// Tracing is disabled by default.
func EnableTracing() Option {
	return func(po *postgresOptions) {
		po.logger = &tracingLogger{}
	}
}

// DebugAnalyzeBeforeStatistics signals to the Statistics method that it should
// run Analyze on the database before returning statistics. This should only be
// used for debug and testing.
//
// Disabled by default.
func DebugAnalyzeBeforeStatistics() Option {
	return func(po *postgresOptions) {
		po.analyzeBeforeStatistics = true
	}
}
