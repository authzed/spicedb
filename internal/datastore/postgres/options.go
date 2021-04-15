package postgres

import (
	"fmt"
	"time"
)

type postgresOptions struct {
	connMaxIdleTime   *time.Duration
	connMaxLifetime   *time.Duration
	healthCheckPeriod *time.Duration
	maxOpenConns      *int
	minOpenConns      *int

	watchBufferLength        uint16
	revisionFuzzingTimedelta time.Duration
	gcWindow                 time.Duration

	enablePrometheusStats bool

	driver string
}

const (
	errFuzzingTooLarge = "revision fuzzing timdelta (%s) must be less than GC window (%s)"

	defaultWatchBufferLength = 128
)

type PostgresOption func(*postgresOptions)

func generateConfig(options []PostgresOption) (postgresOptions, error) {
	computed := postgresOptions{
		gcWindow:          24 * time.Hour,
		watchBufferLength: defaultWatchBufferLength,
		driver:            "postgres",
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

func ConnMaxIdleTime(idle time.Duration) PostgresOption {
	return func(po *postgresOptions) {
		po.connMaxIdleTime = &idle
	}
}

func ConnMaxLifetime(lifetime time.Duration) PostgresOption {
	return func(po *postgresOptions) {
		po.connMaxLifetime = &lifetime
	}
}

func HealthCheckPeriod(period time.Duration) PostgresOption {
	return func(po *postgresOptions) {
		po.healthCheckPeriod = &period
	}
}

func MaxOpenConns(conns int) PostgresOption {
	return func(po *postgresOptions) {
		po.maxOpenConns = &conns
	}
}

func MinOpenConns(conns int) PostgresOption {
	return func(po *postgresOptions) {
		po.minOpenConns = &conns
	}
}

func WatchBufferLength(watchBufferLength uint16) PostgresOption {
	return func(po *postgresOptions) {
		po.watchBufferLength = watchBufferLength
	}
}

func RevisionFuzzingTimedelta(delta time.Duration) PostgresOption {
	return func(po *postgresOptions) {
		po.revisionFuzzingTimedelta = delta
	}
}

func GCWindow(window time.Duration) PostgresOption {
	return func(po *postgresOptions) {
		po.gcWindow = window
	}
}

func EnablePrometheusStats() PostgresOption {
	return func(po *postgresOptions) {
		po.enablePrometheusStats = true
	}
}

func EnableTracing() PostgresOption {
	return func(po *postgresOptions) {
		po.driver = tracingDriverName
	}
}
