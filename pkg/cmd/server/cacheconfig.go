package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/jzelinskie/stringz"
	"github.com/pbnjay/memory"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/pkg/cache"
)

// At startup, measure 75% of available free memory.
var freeMemory uint64

func init() {
	freeMemory = memory.FreeMemory() / 100 * 75
}

// CacheConfig defines configuration for a ristretto cache.
// See: https://github.com/dgraph-io/ristretto#Config
type CacheConfig struct {
	MaxCost     string
	NumCounters int64
	Metrics     bool
	Disabled    bool
}

const (
	defaultMaxCost     = "16MB"
	defaultNumCounters = 1e4 // number of keys to track frequency of (10k).
	defaultBufferItems = 64
)

// Complete translates the CLI cache config into a cache config.
func (cc *CacheConfig) Complete() (*cache.Config, error) {
	if cc.Disabled {
		return &cache.Config{
			MaxCost:     1,
			NumCounters: cc.NumCounters,
			Metrics:     cc.Metrics,
			BufferItems: defaultBufferItems,
			Disabled:    true,
		}, nil
	}

	if cc.MaxCost == "" || cc.NumCounters == 0 {
		return nil, nil
	}

	var (
		maxCost uint64
		err     error
	)

	if strings.HasSuffix(cc.MaxCost, "%") {
		maxCost, err = parsePercent(cc.MaxCost)
	} else {
		maxCost, err = humanize.ParseBytes(cc.MaxCost)
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing cache max memory: `%s`: %w", cc.MaxCost, err)
	}

	return &cache.Config{
		MaxCost:     int64(maxCost),
		NumCounters: cc.NumCounters,
		Metrics:     cc.Metrics,
		BufferItems: defaultBufferItems,
		Disabled:    false,
	}, nil
}

func parsePercent(str string) (uint64, error) {
	percent := strings.TrimSuffix(str, "%")
	parsedPercent, err := strconv.ParseUint(percent, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse percentage: %w", err)
	}

	if parsedPercent > 100 {
		return 0, fmt.Errorf("percentage greater than 100")
	}

	return parsedPercent, nil
}

// RegisterCacheConfigFlags registers flags for a ristretto-based cache.
func RegisterCacheConfigFlags(flags *pflag.FlagSet, config *CacheConfig, flagPrefix string) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "cache")
	flags.StringVar(&config.MaxCost, flagPrefix+"-max-cost", defaultMaxCost, "the maximum cost to be stored in the cache, in bytes")
	flags.Int64Var(&config.NumCounters, flagPrefix+"-num-counters", defaultNumCounters, "the number of keys to track")
	flags.BoolVar(&config.Metrics, flagPrefix+"-metrics", false, "whether metrics should be maintained for the cache. WARNING: Incurs a performance penality.")
	flags.BoolVar(&config.Disabled, flagPrefix+"-disabled", false, "if true, fully disables the cache. WARNING: Incurs a performance penality.")
}
