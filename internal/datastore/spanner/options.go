package spanner

import (
	"fmt"
	"time"
)

type spannerOptions struct {
	watchBufferLength           uint16
	revisionQuantization        time.Duration
	followerReadDelay           time.Duration
	maxRevisionStalenessPercent float64
	gcWindow                    time.Duration
	gcInterval                  time.Duration
	gcEnabled                   bool
	credentialsFilePath         string
	emulatorHost                string
}

const (
	errQuantizationTooLarge = "revision quantization (%s) must be less than GC window (%s)"

	defaultRevisionQuantization        = 5 * time.Second
	defaultFollowerReadDelay           = 0 * time.Second
	defaultMaxRevisionStalenessPercent = 0.1
	defaultWatchBufferLength           = 128
	defaultGCWindow                    = 60 * time.Minute
	defaultGCInterval                  = 3 * time.Minute
	defaultGCEnabled                   = true
)

// Option provides the facility to configure how clients within the Spanner
// datastore interact with the running Spanner database.
type Option func(*spannerOptions)

func generateConfig(options []Option) (spannerOptions, error) {
	computed := spannerOptions{
		gcWindow:                    defaultGCWindow,
		gcInterval:                  defaultGCInterval,
		gcEnabled:                   defaultGCEnabled,
		watchBufferLength:           defaultWatchBufferLength,
		revisionQuantization:        defaultRevisionQuantization,
		followerReadDelay:           defaultFollowerReadDelay,
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

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(so *spannerOptions) {
		so.watchBufferLength = watchBufferLength
	}
}

// RevisionQuantization is the time bucket size to which advertised revisions
// will be rounded.
//
// This value defaults to 5 seconds.
func RevisionQuantization(bucketSize time.Duration) Option {
	return func(so *spannerOptions) {
		so.revisionQuantization = bucketSize
	}
}

// FollowerReadDelay is the time delay to apply to enable historial reads.
//
// This value defaults to 0 seconds.
func FollowerReadDelay(delay time.Duration) Option {
	return func(so *spannerOptions) {
		so.followerReadDelay = delay
	}
}

// MaxRevisionStalenessPercent is the amount of time, expressed as a percentage of
// the revision quantization window, that a previously computed rounded revision
// can still be advertised after the next rounded revision would otherwise be ready.
//
// This value defaults to 0.1 (10%).
func MaxRevisionStalenessPercent(stalenessPercent float64) Option {
	return func(so *spannerOptions) {
		so.maxRevisionStalenessPercent = stalenessPercent
	}
}

// GCWindow is the maximum age of a passed revision that will be considered
// valid.
//
// This value defaults to 1 hour.
func GCWindow(window time.Duration) Option {
	return func(so *spannerOptions) {
		so.gcWindow = window
	}
}

// GCInterval is the the interval at which garbage collection will occur.
//
// This value defaults to 3 minutes.
func GCInterval(interval time.Duration) Option {
	return func(so *spannerOptions) {
		so.gcInterval = interval
	}
}

// CredentialsFile is the path to a file containing credentials for a service
// account that can access the cloud spanner instance
func CredentialsFile(path string) Option {
	return func(so *spannerOptions) {
		so.credentialsFilePath = path
	}
}

// EmulatorHost is the URI of a Spanner emulator to connect to for
// development and testing use
func EmulatorHost(uri string) Option {
	return func(so *spannerOptions) {
		so.emulatorHost = uri
	}
}

// GCEnabled indicates whether garbage collection is enabled.
//
// GC is enabled by default.
func GCEnabled(isGCEnabled bool) Option {
	return func(so *spannerOptions) {
		so.gcEnabled = isGCEnabled
	}
}
