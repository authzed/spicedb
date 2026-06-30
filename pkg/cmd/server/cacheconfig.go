package server

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/pkg/cache"
	pkgruntime "github.com/authzed/spicedb/pkg/runtime"
)

// Factor by which we will extend the maximum amount of expected needed TTL
const ttlExtensionFactor = 2.0

var errOverHundredPercent = errors.New("percentage greater than 100")

// CacheConfig defines the configuration various SpiceDB caches.
//
//go:generate go run github.com/ecordell/optgen -output zz_generated.cacheconfig.options.go . CacheConfig
type CacheConfig struct {
	Name    string `debugmap:"visible"`
	MaxCost string `debugmap:"visible"`
	// Deprecated: NumCounters is no longer used with the current cache implementation.
	NumCounters int64         `debugmap:"visible"`
	Metrics     bool          `debugmap:"visible"`
	Enabled     bool          `debugmap:"visible"`
	defaultTTL  time.Duration `debugmap:"visible"`
}

// WithRevisionParameters configures a cache such that all entries are given a TTL
// that will expire safely outside of the quantization window.
func (cc *CacheConfig) WithRevisionParameters(
	quantizationInterval time.Duration,
	followerReadDelay time.Duration,
	maxStalenessPercent float64,
) *CacheConfig {
	maxExpectedLifetime := float64(quantizationInterval.Nanoseconds())*(1+maxStalenessPercent) + float64(followerReadDelay.Nanoseconds())
	cc.defaultTTL = time.Duration(maxExpectedLifetime*ttlExtensionFactor) * time.Nanosecond
	return cc
}

// CompleteCache translates the CLI cache config into a cache. If metrics are
// enabled for the cache, its metrics are registered with the given registerer.
func CompleteCache[K cache.KeyString, V any](registerer prometheus.Registerer, cc *CacheConfig) (cache.Cache[K, V], error) {
	if !cc.Enabled || cc.MaxCost == "" || cc.MaxCost == "0%" {
		return cache.NoopCache[K, V](), nil
	}

	intMaxCost, err := resolveMaxCost(cc, pkgruntime.AvailableMemory())
	if err != nil {
		return nil, err
	}

	if cc.Metrics {
		return cache.NewStandardCacheWithMetrics[K, V](registerer, cc.Name, &cache.Config{
			MaxCost:    intMaxCost,
			DefaultTTL: cc.defaultTTL,
		})
	}

	return cache.NewStandardCache[K, V](&cache.Config{
		MaxCost:    intMaxCost,
		DefaultTTL: cc.defaultTTL,
	})
}

// resolveMaxCost translates the configured MaxCost (an absolute byte value or a
// percentage of available memory) into a concrete byte budget. availableMem is
// the figure to apply percentages against, as reported by
// pkgruntime.AvailableMemory().
func resolveMaxCost(cc *CacheConfig, availableMem uint64) (int64, error) {
	var (
		maxCost uint64
		err     error
	)

	if strings.HasSuffix(cc.MaxCost, "%") {
		if availableMem == 0 {
			// A percent-based budget against undeterminable available memory
			// resolves to a zero MaxCost, which is a degenerate cache config
			// (e.g. on AWS ECS where the container memory limit isn't written
			// to the cgroup). Fail loudly rather than silently building a
			// useless or unbounded cache.
			return 0, fmt.Errorf("cache %q is configured as a percentage of available memory (%q), but available memory could not be determined; set %s as an absolute byte value (e.g. 1GiB) or ensure GOMEMLIMIT / the container cgroup memory limit is set", cc.Name, cc.MaxCost, cc.Name)
		}
		maxCost, err = parsePercent(cc.MaxCost, availableMem)
	} else {
		maxCost, err = humanize.ParseBytes(cc.MaxCost)
	}
	if err != nil {
		return 0, fmt.Errorf("error parsing cache max memory: `%s`: %w", cc.MaxCost, err)
	}

	intMaxCost, err := safecast.Convert[int64](maxCost)
	if err != nil {
		return 0, errors.New("could not cast max cost to int64")
	}
	return intMaxCost, nil
}

func parsePercent(str string, freeMem uint64) (uint64, error) {
	percent := strings.TrimSuffix(str, "%")
	parsedPercent, err := strconv.ParseUint(percent, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse percentage: %w", err)
	}

	if parsedPercent > 100 {
		return 0, errOverHundredPercent
	}

	return freeMem / 100 * parsedPercent, nil
}

// RegisterCacheFlags registers flags used to configure SpiceDB's various caches.
func RegisterCacheFlags(flags *pflag.FlagSet, flagPrefix, flagDescription string, config, defaults *CacheConfig) error {
	config.Name = defaults.Name
	flagPrefix = cmp.Or(flagPrefix, "cache")
	flags.StringVar(&config.MaxCost, flagPrefix+"-max-cost", defaults.MaxCost, "upper bound (in bytes or as a percent of available memory) of the cache for "+flagDescription)
	flags.Int64Var(&config.NumCounters, flagPrefix+"-num-counters", 0, "number of counters for tracking access frequency in the cache for "+flagDescription+". A higher number means more accurate eviction decisions but more memory usage")
	err := flags.MarkDeprecated(flagPrefix+"-num-counters", "this flag is now unused")
	if err != nil {
		return err
	}
	flags.BoolVar(&config.Metrics, flagPrefix+"-metrics", defaults.Metrics, "enable metrics for the cache for "+flagDescription)
	flags.BoolVar(&config.Enabled, flagPrefix+"-enabled", defaults.Enabled, "enable caching of "+flagDescription)

	return nil
}
