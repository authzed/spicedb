package postgres

import (
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Based off of dbstats_collector
// https://github.com/prometheus/client_golang/blob/master/prometheus/collectors/dbstats_collector.go
type pgxpoolStatsCollector struct {
	dbpool *pgxpool.Pool

	acquireCount         *prometheus.Desc
	acquireDuration      *prometheus.Desc
	acquiredConns        *prometheus.Desc
	canceledAcquireCount *prometheus.Desc
	constructingConns    *prometheus.Desc
	emptyAcquireCount    *prometheus.Desc
	idleConns            *prometheus.Desc
	maxConns             *prometheus.Desc
	totalConns           *prometheus.Desc
}

// Returns a collector that exports stats available in pgxpool
// https://pkg.go.dev/github.com/jackc/pgx/v4/pgxpool#Stat
func NewPgxpoolStatsCollector(dbpool *pgxpool.Pool, dbName string) prometheus.Collector {
	fqName := func(name string) string {
		return "pgxpool_db_" + name
	}
	return &pgxpoolStatsCollector{
		dbpool: dbpool,
		acquireCount: prometheus.NewDesc(
			fqName("acquire_count"),
			"The cumulative count of successful acquires from the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		acquireDuration: prometheus.NewDesc(
			fqName("acquire_duration"),
			"The total duration of all successful acquires from the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		acquiredConns: prometheus.NewDesc(
			fqName("acquire_conns"),
			"The number of currently acquired connections in the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		canceledAcquireCount: prometheus.NewDesc(
			fqName("canceled_acquire_count"),
			"The cumulative count of acquires from the pool that were canceled by a context.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		constructingConns: prometheus.NewDesc(
			fqName("constructing_conns"),
			"The number of conns with construction in progress in the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		emptyAcquireCount: prometheus.NewDesc(
			fqName("empty_acquire_count"),
			"The cumulative count of successful acquires from the pool that waited for a resource to be released or constructed because the pool was empty.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		idleConns: prometheus.NewDesc(
			fqName("idle_conns"),
			"The number of currently idle conns in the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		maxConns: prometheus.NewDesc(
			fqName("max_conns"),
			"The maximum size of the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
		totalConns: prometheus.NewDesc(
			fqName("total_conns"),
			"The total number of resources currently in the pool.",
			nil, prometheus.Labels{"db_name": dbName},
		),
	}
}

func (c *pgxpoolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquireCount
	ch <- c.acquireDuration
	ch <- c.acquiredConns
	ch <- c.canceledAcquireCount
	ch <- c.constructingConns
	ch <- c.emptyAcquireCount
	ch <- c.idleConns
	ch <- c.maxConns
	ch <- c.totalConns
}

func (c *pgxpoolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.dbpool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquireCount, prometheus.CounterValue, float64(stats.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireDuration, prometheus.CounterValue, stats.AcquireDuration().Seconds())
	ch <- prometheus.MustNewConstMetric(c.acquiredConns, prometheus.GaugeValue, float64(stats.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquireCount, prometheus.CounterValue, float64(stats.CanceledAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.constructingConns, prometheus.GaugeValue, float64(stats.ConstructingConns()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquireCount, prometheus.CounterValue, float64(stats.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(stats.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(stats.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(stats.TotalConns()))
}
