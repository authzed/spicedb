// Package embedded runs SpiceDB's permission engine in-process, without standing up a
// gRPC server. It is intended for callers that embed SpiceDB as a library and want to
// issue permission checks directly against a datastore, paying neither network nor
// gRPC-serialization cost, and passing caveat context as native Go values.
package embedded

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/cache"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	defaultConcurrencyLimit  = 10
	defaultDispatchChunkSize = 100
	defaultMaxDepth          = 50
)

// Config configures a Permissions checker.
type Config struct {
	// Datastore is the datastore to check against. Required. The caller owns its lifecycle.
	Datastore datastore.Datastore

	// CaveatTypeSet is the caveat type set used to compile and evaluate caveats.
	// Defaults to caveattypes.Default.
	CaveatTypeSet *caveattypes.TypeSet

	// SchemaMode controls how schema is read. The zero value reads legacy per-definition
	// schema. Use datalayer.SchemaModeReadNewWriteNew (or *Both) for the unified schema.
	SchemaMode datalayer.SchemaMode

	// SchemaCacheMaxCostBytes, when > 0, enables an in-memory stored-schema cache of the
	// given size in bytes. Caching the stored schema across checks is what allows
	// schema-derived caches (e.g. compiled caveats) to persist; strongly recommended when
	// SchemaMode reads from the unified schema.
	SchemaCacheMaxCostBytes int64

	// DispatchConcurrencyLimit, DispatchChunkSize, and MaxDepth use sane defaults if zero.
	DispatchConcurrencyLimit uint16
	DispatchChunkSize        uint16
	MaxDepth                 uint32
}

// Permissions issues in-process permission checks against a datastore.
type Permissions struct {
	dl         datalayer.DataLayer
	dispatcher dispatch.Dispatcher
	cts        *caveattypes.TypeSet
	chunkSize  uint16
	maxDepth   uint32
}

// NewPermissions builds an in-process permissions checker from the given config.
func NewPermissions(cfg Config) (*Permissions, error) {
	if cfg.Datastore == nil {
		return nil, errors.New("embedded: Datastore is required")
	}

	cts := cfg.CaveatTypeSet
	if cts == nil {
		cts = caveattypes.Default.TypeSet
	}
	concurrency := cfg.DispatchConcurrencyLimit
	if concurrency == 0 {
		concurrency = defaultConcurrencyLimit
	}
	chunkSize := cfg.DispatchChunkSize
	if chunkSize == 0 {
		chunkSize = defaultDispatchChunkSize
	}
	maxDepth := cfg.MaxDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxDepth
	}

	dlOpts := []datalayer.DataLayerOption{datalayer.WithSchemaMode(cfg.SchemaMode)}
	if cfg.SchemaCacheMaxCostBytes > 0 {
		schemaCache, err := cache.NewStandardCache[datalayer.SchemaCacheKey, *datalayer.CachedSchema](&cache.Config{
			MaxCost: cfg.SchemaCacheMaxCostBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("embedded: failed to create stored schema cache: %w", err)
		}
		dlOpts = append(dlOpts, datalayer.WithSchemaCache(schemaCache))
	}
	dl := datalayer.NewDataLayer(cfg.Datastore, dlOpts...)

	dispatcher, err := graph.NewLocalOnlyDispatcher(graph.DispatcherParameters{
		ConcurrencyLimits: graph.SharedConcurrencyLimits(concurrency),
		DispatchChunkSize: chunkSize,
		TypeSet:           cts,
	})
	if err != nil {
		return nil, fmt.Errorf("embedded: failed to create dispatcher: %w", err)
	}

	return &Permissions{
		dl:         dl,
		dispatcher: dispatcher,
		cts:        cts,
		chunkSize:  chunkSize,
		maxDepth:   maxDepth,
	}, nil
}

// CheckRequest is a single, fully-consistent permission check. The caveat context is passed
// as native Go values (no structpb / base64 round-trip).
type CheckRequest struct {
	ResourceType    string
	ResourceID      string
	Permission      string
	SubjectType     string
	SubjectID       string
	SubjectRelation string // optional; defaults to the ellipsis ("...") relation
	CaveatContext   map[string]any
}

// CheckResult is the outcome of a check.
type CheckResult struct {
	// HasPermission is true when the subject is definitively a member of the permission.
	HasPermission bool
	// IsConditional is true when membership depends on a caveat that could not be fully
	// evaluated because required context was missing (see MissingContext).
	IsConditional bool
	// MissingContext lists the caveat context fields that were required but not provided.
	MissingContext []string
}

// Check runs a single, fully-consistent permission check in-process.
func (p *Permissions) Check(ctx context.Context, req CheckRequest) (CheckResult, error) {
	ctx = datalayer.ContextWithDataLayer(ctx, p.dl)

	// The datalayer head revision yields the schema-mode-appropriate schema hash (a real
	// hash under the unified schema, or a legacy sentinel otherwise).
	revision, schemaHash, err := p.dl.HeadRevision(ctx)
	if err != nil {
		return CheckResult{}, err
	}

	subjectRel := req.SubjectRelation
	if subjectRel == "" {
		subjectRel = tuple.Ellipsis
	}

	cr, _, err := computed.ComputeCheck(ctx, p.dispatcher, p.cts,
		computed.CheckParameters{
			ResourceType:  tuple.RR(req.ResourceType, req.Permission),
			Subject:       tuple.ONR(req.SubjectType, req.SubjectID, subjectRel),
			CaveatContext: req.CaveatContext,
			AtRevision:    revision,
			MaximumDepth:  p.maxDepth,
			DebugOption:   computed.NoDebugging,
			SchemaHash:    schemaHash,
		},
		req.ResourceID,
		p.chunkSize,
	)
	if err != nil {
		return CheckResult{}, err
	}

	switch cr.Membership {
	case dispatchv1.ResourceCheckResult_MEMBER:
		return CheckResult{HasPermission: true}, nil
	case dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
		return CheckResult{IsConditional: true, MissingContext: cr.MissingExprFields}, nil
	default:
		return CheckResult{}, nil
	}
}

// Close releases resources held by the checker. It does not close the datastore.
func (p *Permissions) Close() error {
	return p.dispatcher.Close()
}
