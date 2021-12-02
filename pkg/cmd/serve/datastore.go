package serve

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

type engineBuilderFunc func(options Options) (datastore.Datastore, error)

func RegisterEngine(key datastore.Engine, builder engineBuilderFunc) {
	if builderForEngine == nil {
		builderForEngine = make(map[datastore.Engine]engineBuilderFunc)
	}
	if _, ok := builderForEngine[key]; ok {
		panic("cannot register two datastore engines with the same name: " + key)
	}
	builderForEngine[key] = builder
}

var builderForEngine map[datastore.Engine]engineBuilderFunc

type Options struct {
	URI                  string
	GCWindow             time.Duration
	RevisionQuantization time.Duration

	MaxIdleTime    time.Duration
	MaxLifetime    time.Duration
	MaxOpenConns   int
	MinOpenConns   int
	SplitQuerySize string

	// CRDB
	FollowerReadDelay time.Duration
	MaxRetries        int
	OverlapKey        string
	OverlapStrategy   string

	// Postgres
	HealthCheckPeriod  time.Duration
	GCInterval         time.Duration
	GCMaxOperationTime time.Duration
}

// RegisterDatastoreFlags adds datastore flags to a cobra command
func RegisterDatastoreFlags(cmd *cobra.Command, opts *Options) {
	cmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	cmd.Flags().StringVar(&opts.URI, "datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)
	cmd.Flags().IntVar(&opts.MaxOpenConns, "datastore-conn-max-open", 20, "number of concurrent connections open in a remote datastore's connection pool")
	cmd.Flags().IntVar(&opts.MinOpenConns, "datastore-conn-min-open", 10, "number of minimum concurrent connections open in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.MaxLifetime, "datastore-conn-max-lifetime", 30*time.Minute, "maximum amount of time a connection can live in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.MaxIdleTime, "datastore-conn-max-idletime", 30*time.Minute, "maximum amount of time a connection can idle in a remote datastore's connection pool")
	cmd.Flags().DurationVar(&opts.HealthCheckPeriod, "datastore-conn-healthcheck-interval", 30*time.Second, "time between a remote datastore's connection pool health checks")
	cmd.Flags().DurationVar(&opts.GCWindow, "datastore-gc-window", 24*time.Hour, "amount of time before revisions are garbage collected")
	cmd.Flags().DurationVar(&opts.GCInterval, "datastore-gc-interval", 3*time.Minute, "amount of time between passes of garbage collection (postgres driver only)")
	cmd.Flags().DurationVar(&opts.GCMaxOperationTime, "datastore-gc-max-operation-time", 1*time.Minute, "maximum amount of time a garbage collection pass can operate before timing out (postgres driver only)")
	cmd.Flags().DurationVar(&opts.RevisionQuantization, "datastore-revision-fuzzing-duration", 5*time.Second, "amount of time to advertize stale revisions")
	// See crdb doc for info about follower reads and how it is configured: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	cmd.Flags().DurationVar(&opts.FollowerReadDelay, "datastore-follower-read-delay-duration", 4_800*time.Millisecond, "amount of time to subtract from non-sync revision timestamps to ensure they are sufficiently in the past to enable follower reads (cockroach driver only)")
	cmd.Flags().StringVar(&opts.SplitQuerySize, "datastore-query-split-size", common.DefaultSplitAtEstimatedQuerySize.String(), "estimated number of bytes at which a query is split when using a remote datastore")
	cmd.Flags().IntVar(&opts.MaxRetries, "datastore-max-tx-retries", 50, "number of times a retriable transaction should be retried (cockroach driver only)")
	cmd.Flags().StringVar(&opts.OverlapStrategy, "datastore-tx-overlap-strategy", "static", `strategy to generate transaction overlap keys ("prefix", "static", "insecure") (cockroach driver only)`)
	cmd.Flags().StringVar(&opts.OverlapKey, "datastore-tx-overlap-key", "key", "static key to touch when writing to ensure transactions overlap (only used if --datastore-tx-overlap-strategy=static is set; cockroach driver only)")
	return
}

type Option func(*Options)

// NewDatastore initializes a datastore given the options
func NewDatastore(kind datastore.Engine, options ...Option) (datastore.Datastore, error) {
	var opts Options
	for _, o := range options {
		o(&opts)
	}

	dsBuilder, ok := builderForEngine[kind]
	if !ok {
		return nil, fmt.Errorf("unknown datastore engine type: %s", kind)
	}
	log.Info().Msgf("using %s datastore engine", kind)

	return dsBuilder(opts)
}

func WithRevisionQuantization(revisionQuantization time.Duration) Option {
	return func(c *Options) {
		c.RevisionQuantization = revisionQuantization
	}
}

func WithGCWindow(gcWindow time.Duration) Option {
	return func(c *Options) {
		c.GCWindow = gcWindow
	}
}

func WithURI(uri string) Option {
	return func(c *Options) {
		c.URI = uri
	}
}

func WithMaxIdleTime(maxIdleTime time.Duration) Option {
	return func(c *Options) {
		c.MaxIdleTime = maxIdleTime
	}
}

func WithMaxLifetime(maxLifetime time.Duration) Option {
	return func(c *Options) {
		c.MaxLifetime = maxLifetime
	}
}

func WithMaxOpenConns(maxOpenConns int) Option {
	return func(c *Options) {
		c.MaxOpenConns = maxOpenConns
	}
}

func WithMinOpenConns(minOpenConns int) Option {
	return func(c *Options) {
		c.MinOpenConns = minOpenConns
	}
}

func WithSplitQuerySize(splitQuerySize string) Option {
	return func(c *Options) {
		c.SplitQuerySize = splitQuerySize
	}
}

func WithFollowerReadDelay(followerDelay time.Duration) Option {
	return func(c *Options) {
		c.FollowerReadDelay = followerDelay
	}
}

func WithMaxRetries(retries int) Option {
	return func(c *Options) {
		c.MaxRetries = retries
	}
}

func WithOverlapKey(key string) Option {
	return func(c *Options) {
		c.OverlapKey = key
	}
}

func WithOverlapStrategy(strategy string) Option {
	return func(c *Options) {
		c.OverlapStrategy = strategy
	}
}

func WithHealthCheckPeriod(interval time.Duration) Option {
	return func(c *Options) {
		c.HealthCheckPeriod = interval
	}
}

func WithGCInterval(interval time.Duration) Option {
	return func(c *Options) {
		c.GCInterval = interval
	}
}

func WithGCMaxOperationTime(interval time.Duration) Option {
	return func(c *Options) {
		c.GCMaxOperationTime = interval
	}
}
