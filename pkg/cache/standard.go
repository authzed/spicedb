package cache

// NewStandardCache creates a new cache with the given configuration.
func NewStandardCache[K KeyString, V any](config *Config) (Cache[K, V], error) {
	// TODO: check name here
	return NewOtterCache[K, V]("", config)
}

func NewStandardCacheWithMetrics[K KeyString, V any](name string, config *Config) (Cache[K, V], error) {
	return NewOtterCacheWithMetrics[K, V](name, config)
}
