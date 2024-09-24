package spanner

import (
	"fmt"
	"math"
	"runtime"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
)

type spannerOptions struct {
	watchBufferLength           uint16
	watchBufferWriteTimeout     time.Duration
	revisionQuantization        time.Duration
	followerReadDelay           time.Duration
	maxRevisionStalenessPercent float64
	credentialsFilePath         string
	credentialsJSON             []byte
	emulatorHost                string
	disableStats                bool
	readMaxOpen                 int
	writeMaxOpen                int
	minSessions                 uint64
	maxSessions                 uint64
	migrationPhase              string
	filterMaximumIDCount        uint16
}

type migrationPhase uint8

const (
	complete migrationPhase = iota
)

var migrationPhases = map[string]migrationPhase{
	"": complete,
}

const (
	errQuantizationTooLarge = "revision quantization (%s) must be less than (%s)"

	defaultRevisionQuantization        = 5 * time.Second
	defaultFollowerReadDelay           = 0 * time.Second
	defaultMaxRevisionStalenessPercent = 0.1
	defaultWatchBufferLength           = 128
	defaultWatchBufferWriteTimeout     = 1 * time.Second
	defaultDisableStats                = false
	maxRevisionQuantization            = 24 * time.Hour
	defaultFilterMaximumIDCount        = 100
)

// Option provides the facility to configure how clients within the Spanner
// datastore interact with the running Spanner database.
type Option func(*spannerOptions)

func generateConfig(options []Option) (spannerOptions, error) {
	// originally SpiceDB didn't use connection pools for Spanner SDK, so it opened 1 single connection
	// This determines if there are more CPU cores to increase the default number of connections
	defaultNumberConnections := max(1, math.Round(float64(runtime.GOMAXPROCS(0))))
	computed := spannerOptions{
		watchBufferLength:           defaultWatchBufferLength,
		watchBufferWriteTimeout:     defaultWatchBufferWriteTimeout,
		revisionQuantization:        defaultRevisionQuantization,
		followerReadDelay:           defaultFollowerReadDelay,
		maxRevisionStalenessPercent: defaultMaxRevisionStalenessPercent,
		disableStats:                defaultDisableStats,
		readMaxOpen:                 int(defaultNumberConnections),
		writeMaxOpen:                int(defaultNumberConnections),
		minSessions:                 100,
		maxSessions:                 400,
		migrationPhase:              "", // no migration
		filterMaximumIDCount:        defaultFilterMaximumIDCount,
	}

	for _, option := range options {
		option(&computed)
	}

	// Run any checks on the config that need to be done
	if computed.revisionQuantization >= maxRevisionQuantization {
		return computed, fmt.Errorf(
			errQuantizationTooLarge,
			computed.revisionQuantization,
			maxRevisionQuantization,
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

// WatchBufferLength is the number of entries that can be stored in the watch
// buffer while awaiting read by the client.
//
// This value defaults to 128.
func WatchBufferLength(watchBufferLength uint16) Option {
	return func(so *spannerOptions) {
		so.watchBufferLength = watchBufferLength
	}
}

// WatchBufferWriteTimeout is the maximum timeout for writing to the watch buffer,
// after which the caller to the watch will be disconnected.
func WatchBufferWriteTimeout(watchBufferWriteTimeout time.Duration) Option {
	return func(so *spannerOptions) { so.watchBufferWriteTimeout = watchBufferWriteTimeout }
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

// FollowerReadDelay is the time delay to apply to enable historical reads.
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

// CredentialsFile is the path to a file containing credentials for a service
// account that can access the cloud spanner instance
func CredentialsFile(path string) Option {
	return func(so *spannerOptions) {
		so.credentialsFilePath = path
	}
}

// CredentialsJSON is the json containing credentials for a service
// account that can access the cloud spanner instance
func CredentialsJSON(json []byte) Option {
	return func(so *spannerOptions) {
		so.credentialsJSON = json
	}
}

// EmulatorHost is the URI of a Spanner emulator to connect to for
// development and testing use
func EmulatorHost(uri string) Option {
	return func(so *spannerOptions) {
		so.emulatorHost = uri
	}
}

// DisableStats disables recording counts to the stats table
func DisableStats(disable bool) Option {
	return func(po *spannerOptions) {
		po.disableStats = disable
	}
}

// ReadConnsMaxOpen is the maximum size of the connection pool used for reads.
//
// This value defaults to having 20 connections.
func ReadConnsMaxOpen(conns int) Option {
	return func(po *spannerOptions) { po.readMaxOpen = conns }
}

// WriteConnsMaxOpen is the maximum size of the connection pool used for writes.
//
// This value defaults to having 10 connections.
func WriteConnsMaxOpen(conns int) Option {
	return func(po *spannerOptions) { po.writeMaxOpen = conns }
}

// MinSessionCount minimum number of session the Spanner client can have
// at a given time.
//
// Defaults to 100.
func MinSessionCount(minSessions uint64) Option {
	return func(po *spannerOptions) { po.minSessions = minSessions }
}

// MaxSessionCount maximum number of session the Spanner client can have
// at a given time.
//
// Defaults to 400 sessions.
func MaxSessionCount(maxSessions uint64) Option {
	return func(po *spannerOptions) { po.maxSessions = maxSessions }
}

// MigrationPhase configures the spanner driver to the proper state of a
// multi-phase migration.
//
// Steady-state configuration (e.g. fully migrated) by default
func MigrationPhase(phase string) Option {
	return func(po *spannerOptions) { po.migrationPhase = phase }
}

// FilterMaximumIDCount is the maximum number of IDs that can be used to filter IDs in queries
func FilterMaximumIDCount(filterMaximumIDCount uint16) Option {
	return func(po *spannerOptions) { po.filterMaximumIDCount = filterMaximumIDCount }
}
