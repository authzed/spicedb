package mysql

import (
	"fmt"
	"time"
)

const (
	errFuzzingTooLarge = "revision fuzzing timedelta (%s) must be less than GC window (%s)"

	defaultGarbageCollectionWindow           = 24 * time.Hour
	defaultGarbageCollectionInterval         = time.Minute * 3
	defaultGarbageCollectionMaxOperationTime = time.Minute
	defaultMaxOpenConns                      = 20
	defaultConnMaxIdleTime                   = 30 * time.Minute
	defaultConnMaxLifetime                   = 30 * time.Minute
	defaultWatchBufferLength                 = 128
	defaultUsersetBatchSize                  = 1024
)

type mysqlOptions struct {
	revisionFuzzingTimedelta time.Duration
	gcWindow                 time.Duration
	gcInterval               time.Duration
	gcMaxOperationTime       time.Duration
	watchBufferLength        uint16
	tablePrefix              string
	enablePrometheusStats    bool
	maxOpenConns             int
	connMaxIdleTime          time.Duration
	connMaxLifetime          time.Duration
	splitAtUsersetCount      int
}

// Option provides the facility to configure how clients within the
// MySQL datastore interact with the running MySQL database.
type Option func(*mysqlOptions)

func generateConfig(options []Option) (mysqlOptions, error) {
	computed := mysqlOptions{
		gcWindow:            defaultGarbageCollectionWindow,
		gcInterval:          defaultGarbageCollectionInterval,
		gcMaxOperationTime:  defaultGarbageCollectionMaxOperationTime,
		watchBufferLength:   defaultWatchBufferLength,
		maxOpenConns:        defaultMaxOpenConns,
		connMaxIdleTime:     defaultConnMaxIdleTime,
		connMaxLifetime:     defaultConnMaxLifetime,
		splitAtUsersetCount: defaultUsersetBatchSize,
	}

	for _, option := range options {
		option(&computed)
	}

	// Run any checks on the config that need to be done
	if computed.revisionFuzzingTimedelta >= computed.gcWindow {
		return computed, fmt.Errorf(
			errFuzzingTooLarge,
			computed.revisionFuzzingTimedelta,
			computed.gcWindow,
		)
	}

	return computed, nil
}

// RevisionFuzzingTimedelta is the time bucket size to which advertised
// revisions will be rounded.
//
// This value defaults to 5 seconds.
func RevisionFuzzingTimedelta(delta time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.revisionFuzzingTimedelta = delta
	}
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

func TablePrefix(prefix string) Option {
	return func(mo *mysqlOptions) {
		mo.tablePrefix = prefix
	}
}

// EnablePrometheusStats enables Prometheus metrics provided by Go's database/sql package
//
// Prometheus metrics are enabled by default.
func EnablePrometheusStats() Option {
	return func(mo *mysqlOptions) {
		mo.enablePrometheusStats = true
	}
}

// ConnMaxIdleTime is the duration after which an idle connection will be
// automatically closed.
// See https://pkg.go.dev/database/sql#DB.SetConnMaxIdleTime/
//
// This value defaults to having no maximum.
func ConnMaxIdleTime(idle time.Duration) Option {
	return func(po *mysqlOptions) {
		po.connMaxIdleTime = idle
	}
}

// ConnMaxLifetime is the duration since creation after which a connection will
// be automatically closed.
// See https://pkg.go.dev/database/sql#DB.SetConnMaxLifetime
//
// This value defaults to having no maximum.
func ConnMaxLifetime(lifetime time.Duration) Option {
	return func(po *mysqlOptions) {
		po.connMaxLifetime = lifetime
	}
}

// MaxOpenConns is the maximum size of the connection pool.
// See https://pkg.go.dev/database/sql#DB.SetMaxOpenConns
//
// This value defaults to having no maximum.
func MaxOpenConns(conns int) Option {
	return func(po *mysqlOptions) {
		po.maxOpenConns = conns
	}
}
