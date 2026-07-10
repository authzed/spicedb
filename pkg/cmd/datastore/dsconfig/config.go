// Package dsconfig holds the configuration type for constructing datastores
// by engine name, along with the registry of engine builders. It is a leaf
// package with minimal dependencies so that datastore engine packages can
// import it to register themselves without creating import cycles;
// pkg/cmd/datastore re-exports everything here and provides flag registration
// and NewDatastore on top.
package dsconfig

import (
	"context"
	"time"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer/schemamode"
	"github.com/authzed/spicedb/pkg/datastore"
)

// EngineBuilderFunc builds a datastore of a specific engine from the given config.
type EngineBuilderFunc func(ctx context.Context, options Config) (datastore.Datastore, error)

const (
	MaxReplicaCount          = 16
	DefaultFollowerReadDelay = 4_800 * time.Millisecond
)

const (
	MemoryEngine    = "memory"
	PostgresEngine  = "postgres"
	CockroachEngine = "cockroachdb"
	SpannerEngine   = "spanner"
	MySQLEngine     = "mysql"
)

// BuilderForEngine holds the builder for each datastore engine, keyed by
// engine name. Engines register themselves via RegisterEngine from an init
// function; importing an engine package makes it available to
// pkg/cmd/datastore.NewDatastore. internal/datastore/engines registers every
// engine defined in this repository.
var BuilderForEngine = map[string]EngineBuilderFunc{}

// RegisterEngine makes a datastore engine available to
// pkg/cmd/datastore.NewDatastore under the given name. It is typically called
// from an init function of the package defining the engine.
func RegisterEngine(engineName string, builder EngineBuilderFunc) {
	BuilderForEngine[engineName] = builder
}

//go:generate go run github.com/ecordell/optgen -output zz_generated.connpool.options.go . ConnPoolConfig
type ConnPoolConfig struct {
	MaxIdleTime         time.Duration `debugmap:"visible" default:"30m"`
	MaxLifetime         time.Duration `debugmap:"visible" default:"30m"`
	MaxLifetimeJitter   time.Duration `debugmap:"visible"`
	MaxOpenConns        int           `debugmap:"visible"`
	MinOpenConns        int           `debugmap:"visible"`
	HealthCheckInterval time.Duration `debugmap:"visible" default:"30s"`
	PingTimeout         time.Duration `debugmap:"visible" default:"5s"`
}

func DefaultReadConnPool() *ConnPoolConfig {
	return &ConnPoolConfig{
		MaxLifetime:         30 * time.Minute,
		MaxIdleTime:         30 * time.Minute,
		MaxOpenConns:        20,
		MinOpenConns:        20,
		HealthCheckInterval: 30 * time.Second,
		PingTimeout:         5 * time.Second,
	}
}

func DefaultWriteConnPool() *ConnPoolConfig {
	cfg := DefaultReadConnPool()
	cfg.MaxOpenConns /= 2
	cfg.MinOpenConns /= 2
	return cfg
}

//go:generate go run github.com/ecordell/optgen -sensitive-field-name-matches uri,secure -output zz_generated.options.go . Config
type Config struct {
	Engine                      string        `debugmap:"visible"   default:"memory"`
	URI                         string        `debugmap:"sensitive"`
	GCWindow                    time.Duration `debugmap:"visible"   default:"24h"`
	LegacyFuzzing               time.Duration `debugmap:"visible"   default:"-1ns"`
	RevisionQuantization        time.Duration `debugmap:"visible"   default:"5s"`
	MaxRevisionStalenessPercent float64       `debugmap:"visible"   default:"0.1"`
	CredentialsProviderName     string        `debugmap:"visible"`
	FilterMaximumIDCount        uint16        `debugmap:"hidden"    default:"100"`

	// Options
	ReadConnPool                   ConnPoolConfig `debugmap:"visible"`
	WriteConnPool                  ConnPoolConfig `debugmap:"visible"`
	ReadOnly                       bool           `debugmap:"visible"`
	EnableDatastoreMetrics         bool           `debugmap:"visible" default:"true"`
	DisableStats                   bool           `debugmap:"visible"`
	IncludeQueryParametersInTraces bool           `debugmap:"visible"`

	// Read Replicas
	ReadReplicaConnPool ConnPoolConfig `debugmap:"visible"`
	// this holds values from the old flag prefix in case they are used
	OldReadReplicaConnPool             ConnPoolConfig `debugmap:"hidden"`
	ReadReplicaURIs                    []string       `debugmap:"sensitive"`
	ReadReplicaCredentialsProviderName string         `debugmap:"visible"`

	// Bootstrap
	BootstrapFiles        []string             `debugmap:"visible-format"`
	BootstrapFileContents map[string][]byte    `debugmap:"visible"`
	BootstrapOverwrite    bool                 `debugmap:"visible"`
	BootstrapTimeout      time.Duration        `debugmap:"visible"        default:"10s"`
	CaveatTypeSet         *caveattypes.TypeSet `debugmap:"hidden"`
	// BootstrapSchemaMode controls the schema storage mode used when writing bootstrap
	// data. The zero value (SchemaModeReadLegacyWriteLegacy) preserves prior behavior.
	BootstrapSchemaMode schemamode.SchemaMode `debugmap:"visible"`

	// Hedging
	RequestHedgingEnabled          bool          `debugmap:"visible"`
	RequestHedgingInitialSlowValue time.Duration `debugmap:"visible"`
	RequestHedgingMaxRequests      uint64        `debugmap:"visible"`
	RequestHedgingQuantile         float64       `debugmap:"visible"`

	// CRDB
	FollowerReadDelay         time.Duration `debugmap:"visible" default:"4800ms"`
	MaxRetries                int           `debugmap:"visible" default:"10"`
	OverlapKey                string        `debugmap:"visible" default:"key"`
	OverlapStrategy           string        `debugmap:"visible" default:"static"`
	EnableConnectionBalancing bool          `debugmap:"visible" default:"true"`
	ConnectRate               time.Duration `debugmap:"visible" default:"100ms"`
	WriteAcquisitionTimeout   time.Duration `debugmap:"visible" default:"30ms"`

	// Postgres
	GCInterval            time.Duration `debugmap:"visible" default:"3m"`
	GCMaxOperationTime    time.Duration `debugmap:"visible" default:"1m"`
	RelaxedIsolationLevel bool          `debugmap:"visible"`

	// Spanner
	// SpannerCredentialsFile is a filename reference to a file containing
	// spanner client credentials.
	//
	// Deprecated: Prefer Application Default Credentials for Spanner client credentials:
	// https://docs.cloud.google.com/docs/authentication/client-libraries#adc
	SpannerCredentialsFile string `debugmap:"visible"`
	// SpannerCredentialsJSON is a mechanism for providing client configuration as JSON.
	//
	// Deprecated: Prefer Application Default Credentials for Spanner client credentials:
	// https://docs.cloud.google.com/docs/authentication/client-libraries#adc
	SpannerCredentialsJSON        []byte `debugmap:"sensitive"`
	SpannerEmulatorHost           string `debugmap:"visible"`
	SpannerMinSessions            uint64 `debugmap:"visible"   default:"100"`
	SpannerMaxSessions            uint64 `debugmap:"visible"   default:"400"`
	SpannerDatastoreMetricsOption string `debugmap:"visible"   default:"otel"`

	// MySQL
	TablePrefix string `debugmap:"visible"`

	// Relationship Integrity
	RelationshipIntegrityEnabled     bool            `debugmap:"visible"`
	RelationshipIntegrityCurrentKey  RelIntegrityKey `debugmap:"visible"`
	RelationshipIntegrityExpiredKeys []string        `debugmap:"visible"`

	// Internal
	WatchBufferLength            uint16        `debugmap:"visible" default:"1024"`
	WatchChangeBufferMaximumSize string        `debugmap:"visible" default:"15%"`
	WatchBufferWriteTimeout      time.Duration `debugmap:"visible" default:"1s"`
	WatchConnectTimeout          time.Duration `debugmap:"visible" default:"1s"`
	DisableWatchSupport          bool          `debugmap:"hidden"`

	// Migrations
	MigrationPhase    string   `debugmap:"visible"`
	AllowedMigrations []string `debugmap:"visible"`

	// Experimental
	ExperimentalColumnOptimization bool `debugmap:"visible" default:"true"`
	EnableRevisionHeartbeat        bool `debugmap:"visible"`
}

// SetDefaults is invoked by github.com/creasty/defaults after struct-tag
// defaults are applied. It fills the four ConnPoolConfig slots from the
// canonical DefaultReadConnPool / DefaultWriteConnPool constructors because
// each slot receives a different default set from RegisterConnPoolFlagsWithPrefix
// in RegisterDatastoreFlagsWithPrefix (Read pools = 20/20 conns, Write = 10/10).
// It also pre-allocates slice fields to empty (non-nil) values so the
// resulting Config matches what RegisterDatastoreFlags writes via
// StringSliceVar/StringArrayVar.
func (c *Config) SetDefaults() {
	c.ReadConnPool = *DefaultReadConnPool()
	c.WriteConnPool = *DefaultWriteConnPool()
	c.ReadReplicaConnPool = *DefaultReadConnPool()
	c.OldReadReplicaConnPool = *DefaultReadConnPool()

	// CaveatTypeSet is hidden from DebugMap but RegisterDatastoreFlags
	// initializes it from DefaultDatastoreConfig at line 223. Mirror that
	// here so library users get the same value as CLI users.
	if c.CaveatTypeSet == nil {
		c.CaveatTypeSet = caveattypes.Default.TypeSet
	}

	if c.BootstrapFiles == nil {
		c.BootstrapFiles = []string{}
	}
	if c.ReadReplicaURIs == nil {
		c.ReadReplicaURIs = []string{}
	}
	if c.AllowedMigrations == nil {
		c.AllowedMigrations = []string{}
	}
	if c.RelationshipIntegrityExpiredKeys == nil {
		c.RelationshipIntegrityExpiredKeys = []string{}
	}
}

//go:generate go run github.com/ecordell/optgen -sensitive-field-name-matches uri,secure -output zz_generated.relintegritykey.options.go . RelIntegrityKey
type RelIntegrityKey struct {
	KeyID       string `debugmap:"visible"`
	KeyFilename string `debugmap:"visible"`
}

func DefaultDatastoreConfig() *Config {
	return &Config{
		Engine:                           MemoryEngine,
		GCWindow:                         24 * time.Hour,
		LegacyFuzzing:                    -1,
		RevisionQuantization:             5 * time.Second,
		MaxRevisionStalenessPercent:      .1, // 10%
		ReadConnPool:                     *DefaultReadConnPool(),
		WriteConnPool:                    *DefaultWriteConnPool(),
		ReadReplicaConnPool:              *DefaultReadConnPool(),
		OldReadReplicaConnPool:           *DefaultReadConnPool(),
		ReadReplicaURIs:                  []string{},
		ReadOnly:                         false,
		MaxRetries:                       10,
		OverlapKey:                       "key",
		OverlapStrategy:                  "static",
		ConnectRate:                      100 * time.Millisecond,
		EnableConnectionBalancing:        true,
		GCInterval:                       3 * time.Minute,
		GCMaxOperationTime:               1 * time.Minute,
		WatchBufferLength:                1024,
		WatchChangeBufferMaximumSize:     "15%",
		WatchBufferWriteTimeout:          1 * time.Second,
		WatchConnectTimeout:              1 * time.Second,
		DisableWatchSupport:              false,
		EnableDatastoreMetrics:           true,
		DisableStats:                     false,
		BootstrapFiles:                   []string{},
		BootstrapTimeout:                 10 * time.Second,
		BootstrapOverwrite:               false,
		SpannerCredentialsFile:           "",
		SpannerEmulatorHost:              "",
		TablePrefix:                      "",
		MigrationPhase:                   "",
		FollowerReadDelay:                DefaultFollowerReadDelay,
		SpannerMinSessions:               100,
		SpannerMaxSessions:               400,
		FilterMaximumIDCount:             100,
		SpannerDatastoreMetricsOption:    "otel",
		RelationshipIntegrityEnabled:     false,
		RelationshipIntegrityCurrentKey:  RelIntegrityKey{},
		RelationshipIntegrityExpiredKeys: []string{},
		AllowedMigrations:                []string{},
		ExperimentalColumnOptimization:   true,
		IncludeQueryParametersInTraces:   false,
		WriteAcquisitionTimeout:          30 * time.Millisecond,
		CaveatTypeSet:                    caveattypes.Default.TypeSet,
	}
}
