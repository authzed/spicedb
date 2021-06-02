package crdb

import (
	"fmt"
	"time"
)

type crdbOptions struct {
	connMaxIdleTime *time.Duration
	connMaxLifetime *time.Duration
	minOpenConns    *int
	maxOpenConns    *int

	watchBufferLength    uint16
	revisionQuantization time.Duration
	gcWindow             time.Duration

	enablePrometheusStats bool
}

const (
	errQuantizationTooLarge = "revision quantization (%s) must be less than GC window (%s)"

	defaultRevisionQuantization = 5 * time.Second
	defaultWatchBufferLength    = 128
)

type CRDBOption func(*crdbOptions)

func generateConfig(options []CRDBOption) (crdbOptions, error) {
	computed := crdbOptions{
		gcWindow:             24 * time.Hour,
		watchBufferLength:    defaultWatchBufferLength,
		revisionQuantization: defaultRevisionQuantization,
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

func ConnMaxIdleTime(idle time.Duration) CRDBOption {
	return func(po *crdbOptions) {
		po.connMaxIdleTime = &idle
	}
}

func ConnMaxLifetime(lifetime time.Duration) CRDBOption {
	return func(po *crdbOptions) {
		po.connMaxLifetime = &lifetime
	}
}

func MinOpenConns(conns int) CRDBOption {
	return func(po *crdbOptions) {
		po.minOpenConns = &conns
	}
}

func MaxOpenConns(conns int) CRDBOption {
	return func(po *crdbOptions) {
		po.maxOpenConns = &conns
	}
}

func WatchBufferLength(watchBufferLength uint16) CRDBOption {
	return func(po *crdbOptions) {
		po.watchBufferLength = watchBufferLength
	}
}

func RevisionQuantization(bucketSize time.Duration) CRDBOption {
	return func(po *crdbOptions) {
		po.revisionQuantization = bucketSize
	}
}

func GCWindow(window time.Duration) CRDBOption {
	return func(po *crdbOptions) {
		po.gcWindow = window
	}
}

func EnablePrometheusStats() CRDBOption {
	return func(po *crdbOptions) {
		po.enablePrometheusStats = true
	}
}
