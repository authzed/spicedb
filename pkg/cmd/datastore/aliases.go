package datastore

import (
	"github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
)

// The datastore Config type, its options, and the engine registry live in the
// leaf package dsconfig so that datastore engine packages can import them to
// register themselves without creating import cycles. Everything is
// re-exported here for backwards compatibility.

type (
	Config                = dsconfig.Config
	ConfigOption          = dsconfig.ConfigOption
	ConnPoolConfig        = dsconfig.ConnPoolConfig
	ConnPoolConfigOption  = dsconfig.ConnPoolConfigOption
	RelIntegrityKey       = dsconfig.RelIntegrityKey
	RelIntegrityKeyOption = dsconfig.RelIntegrityKeyOption
	EngineBuilderFunc     = dsconfig.EngineBuilderFunc
)

const (
	MaxReplicaCount          = dsconfig.MaxReplicaCount
	DefaultFollowerReadDelay = dsconfig.DefaultFollowerReadDelay

	MemoryEngine    = dsconfig.MemoryEngine
	PostgresEngine  = dsconfig.PostgresEngine
	CockroachEngine = dsconfig.CockroachEngine
	SpannerEngine   = dsconfig.SpannerEngine
	MySQLEngine     = dsconfig.MySQLEngine
)

// BuilderForEngine holds the builder for each datastore engine, keyed by
// engine name. See dsconfig.BuilderForEngine.
var BuilderForEngine = dsconfig.BuilderForEngine

var (
	RegisterEngine                           = dsconfig.RegisterEngine
	DefaultDatastoreConfig                   = dsconfig.DefaultDatastoreConfig
	DefaultReadConnPool                      = dsconfig.DefaultReadConnPool
	DefaultWriteConnPool                     = dsconfig.DefaultWriteConnPool
	ConfigWithOptions                        = dsconfig.ConfigWithOptions
	ConnPoolConfigWithOptions                = dsconfig.ConnPoolConfigWithOptions
	NewConfigWithOptions                     = dsconfig.NewConfigWithOptions
	NewConfigWithOptionsAndDefaults          = dsconfig.NewConfigWithOptionsAndDefaults
	NewConnPoolConfigWithOptions             = dsconfig.NewConnPoolConfigWithOptions
	NewConnPoolConfigWithOptionsAndDefaults  = dsconfig.NewConnPoolConfigWithOptionsAndDefaults
	NewRelIntegrityKeyWithOptions            = dsconfig.NewRelIntegrityKeyWithOptions
	NewRelIntegrityKeyWithOptionsAndDefaults = dsconfig.NewRelIntegrityKeyWithOptionsAndDefaults
	RelIntegrityKeyWithOptions               = dsconfig.RelIntegrityKeyWithOptions
	SetAllowedMigrations                     = dsconfig.SetAllowedMigrations
	SetBootstrapFileContents                 = dsconfig.SetBootstrapFileContents
	SetBootstrapFiles                        = dsconfig.SetBootstrapFiles
	SetReadReplicaURIs                       = dsconfig.SetReadReplicaURIs
	SetRelationshipIntegrityExpiredKeys      = dsconfig.SetRelationshipIntegrityExpiredKeys
	SetSpannerCredentialsJSON                = dsconfig.SetSpannerCredentialsJSON
	WithAllowedMigrations                    = dsconfig.WithAllowedMigrations
	WithBootstrapFileContents                = dsconfig.WithBootstrapFileContents
	WithBootstrapFiles                       = dsconfig.WithBootstrapFiles
	WithBootstrapOverwrite                   = dsconfig.WithBootstrapOverwrite
	WithBootstrapSchemaMode                  = dsconfig.WithBootstrapSchemaMode
	WithBootstrapTimeout                     = dsconfig.WithBootstrapTimeout
	WithCaveatTypeSet                        = dsconfig.WithCaveatTypeSet
	WithConnectRate                          = dsconfig.WithConnectRate
	WithCredentialsProviderName              = dsconfig.WithCredentialsProviderName
	WithDisableStats                         = dsconfig.WithDisableStats
	WithDisableWatchSupport                  = dsconfig.WithDisableWatchSupport
	WithEnableConnectionBalancing            = dsconfig.WithEnableConnectionBalancing
	WithEnableDatastoreMetrics               = dsconfig.WithEnableDatastoreMetrics
	WithEnableRevisionHeartbeat              = dsconfig.WithEnableRevisionHeartbeat
	WithEngine                               = dsconfig.WithEngine
	WithExperimentalColumnOptimization       = dsconfig.WithExperimentalColumnOptimization
	WithFilterMaximumIDCount                 = dsconfig.WithFilterMaximumIDCount
	WithFollowerReadDelay                    = dsconfig.WithFollowerReadDelay
	WithGCInterval                           = dsconfig.WithGCInterval
	WithGCMaxOperationTime                   = dsconfig.WithGCMaxOperationTime
	WithGCWindow                             = dsconfig.WithGCWindow
	WithHealthCheckInterval                  = dsconfig.WithHealthCheckInterval
	WithIncludeQueryParametersInTraces       = dsconfig.WithIncludeQueryParametersInTraces
	WithKeyFilename                          = dsconfig.WithKeyFilename
	WithKeyID                                = dsconfig.WithKeyID
	WithLegacyFuzzing                        = dsconfig.WithLegacyFuzzing
	WithMaxIdleTime                          = dsconfig.WithMaxIdleTime
	WithMaxLifetime                          = dsconfig.WithMaxLifetime
	WithMaxLifetimeJitter                    = dsconfig.WithMaxLifetimeJitter
	WithMaxOpenConns                         = dsconfig.WithMaxOpenConns
	WithMaxRetries                           = dsconfig.WithMaxRetries
	WithMaxRevisionStalenessPercent          = dsconfig.WithMaxRevisionStalenessPercent
	WithMigrationPhase                       = dsconfig.WithMigrationPhase
	WithMinOpenConns                         = dsconfig.WithMinOpenConns
	WithOldReadReplicaConnPool               = dsconfig.WithOldReadReplicaConnPool
	WithOverlapKey                           = dsconfig.WithOverlapKey
	WithOverlapStrategy                      = dsconfig.WithOverlapStrategy
	WithPingTimeout                          = dsconfig.WithPingTimeout
	WithReadConnPool                         = dsconfig.WithReadConnPool
	WithReadOnly                             = dsconfig.WithReadOnly
	WithReadReplicaConnPool                  = dsconfig.WithReadReplicaConnPool
	WithReadReplicaCredentialsProviderName   = dsconfig.WithReadReplicaCredentialsProviderName
	WithReadReplicaURIs                      = dsconfig.WithReadReplicaURIs
	WithRelationshipIntegrityCurrentKey      = dsconfig.WithRelationshipIntegrityCurrentKey
	WithRelationshipIntegrityEnabled         = dsconfig.WithRelationshipIntegrityEnabled
	WithRelationshipIntegrityExpiredKeys     = dsconfig.WithRelationshipIntegrityExpiredKeys
	WithRelaxedIsolationLevel                = dsconfig.WithRelaxedIsolationLevel
	WithRequestHedgingEnabled                = dsconfig.WithRequestHedgingEnabled
	WithRequestHedgingInitialSlowValue       = dsconfig.WithRequestHedgingInitialSlowValue
	WithRequestHedgingMaxRequests            = dsconfig.WithRequestHedgingMaxRequests
	WithRequestHedgingQuantile               = dsconfig.WithRequestHedgingQuantile
	WithRevisionQuantization                 = dsconfig.WithRevisionQuantization
	WithSpannerCredentialsFile               = dsconfig.WithSpannerCredentialsFile
	WithSpannerCredentialsJSON               = dsconfig.WithSpannerCredentialsJSON
	WithSpannerDatastoreMetricsOption        = dsconfig.WithSpannerDatastoreMetricsOption
	WithSpannerEmulatorHost                  = dsconfig.WithSpannerEmulatorHost
	WithSpannerMaxSessions                   = dsconfig.WithSpannerMaxSessions
	WithSpannerMinSessions                   = dsconfig.WithSpannerMinSessions
	WithTablePrefix                          = dsconfig.WithTablePrefix
	WithURI                                  = dsconfig.WithURI
	WithWatchBufferLength                    = dsconfig.WithWatchBufferLength
	WithWatchBufferWriteTimeout              = dsconfig.WithWatchBufferWriteTimeout
	WithWatchChangeBufferMaximumSize         = dsconfig.WithWatchChangeBufferMaximumSize
	WithWatchConnectTimeout                  = dsconfig.WithWatchConnectTimeout
	WithWriteAcquisitionTimeout              = dsconfig.WithWriteAcquisitionTimeout
	WithWriteConnPool                        = dsconfig.WithWriteConnPool
)
