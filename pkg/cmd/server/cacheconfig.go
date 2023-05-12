package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jzelinskie/stringz"
	"github.com/pbnjay/memory"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/pkg/cache"
)

// Factor by which we will extend the maximum amount of expected needed TTL
const ttlExtensionFactor = 2.0

var (
	// At startup, measure 75% of available free memory.
	freeMemory uint64

	errOverHundredPercent = errors.New("percentage greater than 100")
)

func init() {
	freeMemory = memory.FreeMemory() / 100 * 75
}

// CacheConfig defines the configuration various SpiceDB caches.
//
//go:generate go run github.com/ecordell/optgen -output zz_generated.cacheconfig.options.go . CacheConfig
type CacheConfig struct {
	Name        string        `debugmap:"visible"`
	MaxCost     string        `debugmap:"visible"`
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

// Complete translates the CLI cache config into a cache config.
func (cc *CacheConfig) Complete() (cache.Cache, error) {
	if !cc.Enabled || cc.MaxCost == "" || cc.MaxCost == "0%" || cc.NumCounters == 0 {
		return cache.NoopCache(), nil
	}

	var (
		maxCost uint64
		err     error
	)

	if strings.HasSuffix(cc.MaxCost, "%") {
		maxCost, err = parsePercent(cc.MaxCost, freeMemory)
	} else {
		maxCost, err = humanize.ParseBytes(cc.MaxCost)
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing cache max memory: `%s`: %w", cc.MaxCost, err)
	}

	if cc.Metrics {
		return cache.NewCacheWithMetrics(cc.Name, &cache.Config{
			MaxCost:     int64(maxCost),
			NumCounters: cc.NumCounters,
			DefaultTTL:  cc.defaultTTL,
		})
	}

	return cache.NewCache(&cache.Config{
		MaxCost:     int64(maxCost),
		NumCounters: cc.NumCounters,
		DefaultTTL:  cc.defaultTTL,
	})
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

// RegisterCacheFlags registers flags used to configure SpiceDB's various
// caches.
func RegisterCacheFlags(flags *pflag.FlagSet, flagPrefix string, config, defaults *CacheConfig) {
	config.Name = defaults.Name
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "cache")
	flags.StringVar(&config.MaxCost, flagPrefix+"-max-cost", defaults.MaxCost, "upper bound cache size in bytes or percent of available memory")
	flags.Int64Var(&config.NumCounters, flagPrefix+"-num-counters", defaults.NumCounters, "number of TinyLFU samples to track")
	flags.BoolVar(&config.Metrics, flagPrefix+"-metrics", defaults.Metrics, "enable cache metrics")
	flags.BoolVar(&config.Enabled, flagPrefix+"-enabled", defaults.Enabled, "enable caching")
}
