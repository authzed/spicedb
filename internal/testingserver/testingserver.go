// Package testingserver holds the configuration shared by the hosts of the testing server: the
// `serve-testing` command and the in-process server (and its WASI build). Sharing it keeps the
// API behavior of the hosts identical.
package testingserver

import (
	"time"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/services"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	consistencymw "github.com/authzed/spicedb/pkg/middleware/consistency"
)

const (
	maxDepth                = 50
	defaultConcurrencyLimit = 10
	defaultMaxChunkSize     = 100
	watchHeartbeat          = 1 * time.Second
)

// Defaults for the API behavior settings of the testing server; these are the defaults of the
// `serve-testing` command's flags.
const (
	DefaultMaximumUpdatesPerWrite          uint16 = 1000
	DefaultMaximumPreconditionCount        uint16 = 1000
	DefaultMaxCaveatContextSize                   = 4096
	DefaultMaxRelationshipContextSize             = 25000
	DefaultMaxReadRelationshipsLimit       uint32 = 1000
	DefaultMaxDeleteRelationshipsLimit     uint32 = 1000
	DefaultMaxLookupResourcesLimit         uint32 = 1000
	DefaultMaxBulkExportRelationshipsLimit uint32 = 10_000
)

// The settings of the testing server's consistency middleware.
const (
	ConsistencyServiceLabel   = ""
	MismatchingZedTokenOption = consistencymw.TreatMismatchingTokensAsFullConsistency
)

// Limits are the API behavior settings of the testing server.
type Limits struct {
	MaximumUpdatesPerWrite          uint16
	MaximumPreconditionCount        uint16
	MaxCaveatContextSize            int
	MaxReadRelationshipsLimit       uint32
	MaxDeleteRelationshipsLimit     uint32
	MaxLookupResourcesLimit         uint32
	MaxBulkExportRelationshipsLimit uint32
}

// DefaultLimits returns the limits used when none are configured.
func DefaultLimits() Limits {
	return Limits{
		MaximumUpdatesPerWrite:          DefaultMaximumUpdatesPerWrite,
		MaximumPreconditionCount:        DefaultMaximumPreconditionCount,
		MaxCaveatContextSize:            DefaultMaxCaveatContextSize,
		MaxReadRelationshipsLimit:       DefaultMaxReadRelationshipsLimit,
		MaxDeleteRelationshipsLimit:     DefaultMaxDeleteRelationshipsLimit,
		MaxLookupResourcesLimit:         DefaultMaxLookupResourcesLimit,
		MaxBulkExportRelationshipsLimit: DefaultMaxBulkExportRelationshipsLimit,
	}
}

// RegisterServices registers the testing server's API services on srv.
func RegisterServices(srv *grpc.Server, healthManager health.Manager, dispatcher dispatch.Dispatcher, limits Limits, caveatTypeSet *caveattypes.TypeSet, metrics *v1svc.Metrics) {
	services.RegisterGrpcServices(
		srv,
		healthManager,
		dispatcher,
		services.V1SchemaServiceEnabled,
		services.WatchServiceEnabled,
		v1svc.PermissionsServerConfig{
			Metrics:                         metrics,
			MaxPreconditionsCount:           limits.MaximumPreconditionCount,
			MaxUpdatesPerWrite:              limits.MaximumUpdatesPerWrite,
			MaximumAPIDepth:                 maxDepth,
			MaxCaveatContextSize:            limits.MaxCaveatContextSize,
			MaxReadRelationshipsLimit:       limits.MaxReadRelationshipsLimit,
			MaxDeleteRelationshipsLimit:     limits.MaxDeleteRelationshipsLimit,
			MaxLookupResourcesLimit:         limits.MaxLookupResourcesLimit,
			MaxBulkExportRelationshipsLimit: limits.MaxBulkExportRelationshipsLimit,
			DispatchChunkSize:               defaultMaxChunkSize,
			ExpiringRelationshipsEnabled:    true,
			CaveatTypeSet:                   caveatTypeSet,
		},
		watchHeartbeat,
	)
}
