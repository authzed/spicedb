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

	watchBufferLength           uint16
	revisionQuantization        time.Duration
	followerReadDelay           time.Duration
	maxRevisionStalenessPercent float64
	gcWindow                    time.Duration
	maxRetries                  uint8
	splitAtUsersetCount         uint16
	overlapStrategy             string
	overlapKey                  string
}

const (
	errQuantizationTooLarge = "revision quantization (%s) must be less than GC window (%s)"

	overlapStrategyPrefix   = "prefix"
	overlapStrategyStatic   = "static"
	overlapStrategyInsecure = "insecure"

	defaultRevisionQuantization        = 5 * time.Second
	defaultFollowerReadDelay           = 0 * time.Second
	defaultMaxRevisionStalenessPercent = 0.1
	defaultWatchBufferLength           = 128
	defaultSplitSize                   = 1024

	defaultMaxRetries      = 5
	defaultOverlapKey      = "defaultsynckey"
	defaultOverlapStrategy = overlapStrategyStatic
)

// Option provides the facility to configure how clients within the CRDB
// datastore interact with the running CockroachDB database.
type Option func(*crdbOptions)

func generateConfig(options []Option) (crdbOptions, error) {
	computed := crdbOptions{
		gcWindow:                    24 * time.Hour,
		watchBufferLength:           defaultWatchBufferLength,
		revisionQuantization:        defaultRevisionQuantization,
		followerReadDelay:           defaultFollowerReadDelay,
		maxRevisionStalenessPercent: defaultMaxRevisionStalenessPercent,
		splitAtUsersetCount:         defaultSplitSize,
		maxRetries:                  defaultMaxRetries,
		overlapKey:                  defaultOverlapKey,
		overlapStrategy:             defaultOverlapStrategy,
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
	return func(po *crdbOptions) {
		po.splitAtUsersetCount = splitAtUsersetCount
	}
}

// ConnMaxIdleTime is the duration after which an idle connection will be
// automatically closed by the health check.
//
// This value defaults to having no maximum.
func ConnMaxIdleTime(idle time.Duration) Option {
	return func(po *crdbOptions) {
		po.connMaxIdleTime = &idle
	}
}

// ConnMaxLifetime is the duration since creation after which a connection will
// be automatically closed.
//
// This value defaults to having no maximum.
func ConnMaxLifetime(lifetime time.Duration) Option {
	return func(po *crdbOptions) {
		po.connMaxLifetime = &lifetime
	}
}

// MinOpenConns is the minimum size of the connection pool.
// The health check will increase the number of connections to this amount if
// it had dropped below.
//
// This value defaults to zero.
func MinOpenConns(conns int) Option {
	return func(po *crdbOptions) {
		po.minOpenConns = &conns
	}
}

// MaxOpenConns is the maximum size of the connection pool.
//
// This value defaults to having no maximum.
func MaxOpenConns(conns int) Option {
	return func(po *crdbOptions) {
		po.maxOpenConns = &conns
	}
}

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(po *crdbOptions) {
		po.watchBufferLength = watchBufferLength
	}
}

// RevisionQuantization is the time bucket size to which advertised revisions
// will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(bucketSize time.Duration) Option {
	return func(po *crdbOptions) {
		po.revisionQuantization = bucketSize
	}
}

// FollowerReadDelay is the time delay to apply to enable historial reads.
//
// This value defaults to 0 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(po *crdbOptions) {
		po.followerReadDelay = delay
	}
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(po *crdbOptions) {
		po.maxRevisionStalenessPercent = stalenessPercent
	}
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 24 hours.
func GCWindow(window time.Duration) Option {
	return func(po *crdbOptions) {
		po.gcWindow = window
	}
}

// MaxRetries is the maximum number of times a retriable transaction will be
// client-side retried.
// Default: 5
func MaxRetries(maxRetries uint8) Option {
	return func(po *crdbOptions) {
		po.maxRetries = maxRetries
	}
}

// OverlapStrategy is the strategy used to generate overlap keys on write.
// Default: 'static'
func OverlapStrategy(strategy string) Option {
	return func(po *crdbOptions) {
		po.overlapStrategy = strategy
	}
}

// OverlapKey is a key touched on every write if OverlapStrategy is "static"
// Default: 'key'
func OverlapKey(key string) Option {
	return func(po *crdbOptions) {
		po.overlapKey = key
	}
}
