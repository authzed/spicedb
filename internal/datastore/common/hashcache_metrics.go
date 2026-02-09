package common

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// schemaCacheLookups tracks the total number of schema cache lookups
	schemaCacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "schema_cache_lookups_total",
		Help:      "total number of schema cache Get() calls",
	}, []string{"result"}) // result: "hit" or "miss"

	// schemaCacheHits tracks cache hits
	schemaCacheHits = schemaCacheLookups.WithLabelValues("hit")

	// schemaCacheMisses tracks cache misses
	schemaCacheMisses = schemaCacheLookups.WithLabelValues("miss")
)
