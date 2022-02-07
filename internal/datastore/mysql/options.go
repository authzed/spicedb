package mysql

import (
	"fmt"
	"time"

	"github.com/alecthomas/units"

	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	errFuzzingTooLarge = "revision fuzzing timedelta (%s) must be less than GC window (%s)"

	defaultGarbageCollectionWindow           = 24 * time.Hour
	defaultGarbageCollectionInterval         = time.Minute * 3
	defaultGarbageCollectionMaxOperationTime = time.Minute

	defaultWatchBufferLength = 128
)

type mysqlOptions struct {
	revisionFuzzingTimedelta  time.Duration
	gcWindow                  time.Duration
	gcInterval                time.Duration
	gcMaxOperationTime        time.Duration
	splitAtEstimatedQuerySize units.Base2Bytes
	watchBufferLength         uint16
}

// Option provides the facility to configure how clients within the
// MySQL datastore interact with the running MySQL database.
type Option func(*mysqlOptions)

func generateConfig(options []Option) (mysqlOptions, error) {
	computed := mysqlOptions{
		gcWindow:                  defaultGarbageCollectionWindow,
		gcInterval:                defaultGarbageCollectionInterval,
		gcMaxOperationTime:        defaultGarbageCollectionMaxOperationTime,
		splitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize,
		watchBufferLength:         defaultWatchBufferLength,
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

// SplitAtEstimatedQuerySize is the query size at which it is split into two
// (or more) queries.
//
// This value defaults to `common.DefaultSplitAtEstimatedQuerySize`.
func SplitAtEstimatedQuerySize(splitAtEstimatedQuerySize units.Base2Bytes) Option {
	return func(mo *mysqlOptions) {
		mo.splitAtEstimatedQuerySize = splitAtEstimatedQuerySize
	}
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

// GCInterval is the the interval at which garbage collection will occur.
//
// This value defaults to 3 minutes.
func GCInterval(interval time.Duration) Option {
	return func(mo *mysqlOptions) {
		mo.gcInterval = interval
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
