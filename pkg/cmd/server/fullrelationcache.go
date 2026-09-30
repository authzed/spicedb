package server

import (
	"context"
	"fmt"
	"time"

	"github.com/authzed/spicedb/internal/datastore/proxy/fullrelationcache"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	pkgruntime "github.com/authzed/spicedb/pkg/runtime"
)

// FullRelationCacheMode selects whether the server builds the full relation cache.
type FullRelationCacheMode string

const (
	// FullRelationCacheDisabled does not build the full relation cache.
	FullRelationCacheDisabled FullRelationCacheMode = "disabled"

	// FullRelationCacheEnabled builds the full relation cache.
	FullRelationCacheEnabled FullRelationCacheMode = "enabled"
)

// fullRelationCacheAccessCounterBudget is the size in bytes of the access counter table.
const fullRelationCacheAccessCounterBudget = 8 << 20

// ParseFullRelationCacheMode returns the mode for s. An unknown value returns an error.
func ParseFullRelationCacheMode(s string) (FullRelationCacheMode, error) {
	switch mode := FullRelationCacheMode(s); mode {
	case FullRelationCacheDisabled, FullRelationCacheEnabled:
		return mode, nil
	default:
		return FullRelationCacheDisabled, fmt.Errorf(
			"invalid full relation cache mode %q, must be one of: disabled, enabled", s)
	}
}

// withFullRelationCache wraps ds in the full relation cache proxy if mode is enabled and the cache has a budget.
// It adds the cache and the access counter to closeables.
func (c *Config) withFullRelationCache(ctx context.Context, ds datastore.Datastore,
	mode FullRelationCacheMode, closeables *util.CloseableStack,
) (datastore.Datastore, error) {
	if mode != FullRelationCacheEnabled {
		return ds, nil
	}

	active, err := cacheRetainsEntries(&c.FullRelationCacheConfig, pkgruntime.AvailableMemory())
	if err != nil {
		return nil, fmt.Errorf("failed to create full relation cache: %w", err)
	}
	if !active {
		log.Ctx(ctx).Warn().
			Bool("cache_enabled", c.FullRelationCacheConfig.Enabled).
			Str("max_cost", c.FullRelationCacheConfig.MaxCost).
			Msg("full relation cache is enabled but its cache is disabled or has zero size; the full relation cache is inactive")
		return ds, nil
	}

	if total := c.builtCachePercentTotal(); total > 100 {
		log.Ctx(ctx).Warn().Uint64("total_percent", total).
			Msg("enabled cache memory budgets sum to more than 100% of available memory; consider lowering --dispatch-cluster-cache-max-cost or --experimental-full-relation-cache-max-cost")
	}

	sets, err := CompleteCache[fullrelationcache.SetKey, *fullrelationcache.CachedSet](c.OTel.PrometheusRegistry,
		// Complete sets have revision keys and expire by TTL.
		// A hot too-big memo never expires by TTL, so the proxy re-probes it after a fixed interval.
		c.FullRelationCacheConfig.WithRevisionParameters(
			c.DatastoreConfig.RevisionQuantization,
			c.DatastoreConfig.FollowerReadDelay,
			c.DatastoreConfig.MaxRevisionStalenessPercent,
		))
	if err != nil {
		return nil, fmt.Errorf("failed to create full relation cache: %w", err)
	}
	closeables.AddWithoutError(sets.Close)
	log.Ctx(ctx).Info().EmbedObject(sets).Msg("configured full relation cache")

	counter, err := fullrelationcache.NewAccessCounter(fullRelationCacheAccessCounterBudget, time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to create full relation cache access counter: %w", err)
	}
	closeables.AddWithoutError(counter.Close)

	return fullrelationcache.NewProxy(ds, sets, counter, fullrelationcache.Options{
		MaterializeThreshold: c.FullRelationCacheMaterializeThreshold,
		MaximumSetSize:       c.FullRelationCacheMaximumSetSize,
	}), nil
}
