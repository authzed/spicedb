package otelpgx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

const (
	// defaultMinimumReadDBStatsInterval is the default minimum interval between calls to db.Stats().
	defaultMinimumReadDBStatsInterval = time.Second
)

var (
	pgxPoolAcquireCount            = "pgxpool.acquires"
	pgxPoolAcquireDuration         = "pgxpool.acquire_duration"
	pgxPoolAcquiredConnections     = "pgxpool.acquired_connections"
	pgxPoolCancelledAcquires       = "pgxpool.canceled_acquires"
	pgxPoolConstructingConnections = "pgxpool.constructing_connections"
	pgxPoolEmptyAcquire            = "pgxpool.empty_acquire"
	pgxPoolIdleConnections         = "pgxpool.idle_connections"
	pgxPoolMaxConnections          = "pgxpool.max_connections"
	pgxPoolMaxIdleDestroyCount     = "pgxpool.max_idle_destroys"
	pgxPoolMaxLifetimeDestroyCount = "pgxpool.max_lifetime_destroys"
	pgxPoolNewConnectionsCount     = "pgxpool.new_connections"
	pgxPoolTotalConnections        = "pgxpool.total_connections"
	pgxPoolEmptyAcquireWaitTime    = "pgxpool.empty_acquire_wait_time"
)

// RecordStats records database statistics for provided pgxpool.Pool at a default 1 second interval
// unless otherwise specified by the WithMinimumReadDBStatsInterval StatsOption.
func RecordStats(db PoolStats, opts ...StatsOption) error {
	o := statsOptions{
		meterProvider:              otel.GetMeterProvider(),
		minimumReadDBStatsInterval: defaultMinimumReadDBStatsInterval,
		defaultAttributes: []attribute.KeyValue{
			semconv.DBSystemPostgreSQL,
		},
	}

	for _, opt := range opts {
		opt.applyStatsOptions(&o)
	}

	meter := o.meterProvider.Meter(meterName, metric.WithInstrumentationVersion(findOwnImportedVersion()))

	return recordStats(meter, db, o.minimumReadDBStatsInterval, o.defaultAttributes...)
}

// PoolStats is an interface that provides access to the pgxpool.Pool's statistics.
type PoolStats interface {
	Stat() *pgxpool.Stat
	Config() *pgxpool.Config
}

func recordStats(
	meter metric.Meter,
	db PoolStats,
	minimumReadDBStatsInterval time.Duration,
	attrs ...attribute.KeyValue,
) error {
	var (
		err error

		// Asynchronous Observable Metrics
		acquireCount            metric.Int64ObservableCounter
		acquireDuration         metric.Int64ObservableCounter
		acquiredConns           metric.Int64ObservableUpDownCounter
		cancelledAcquires       metric.Int64ObservableCounter
		constructingConns       metric.Int64ObservableUpDownCounter
		emptyAcquires           metric.Int64ObservableCounter
		idleConns               metric.Int64ObservableUpDownCounter
		maxConns                metric.Int64ObservableGauge
		maxIdleDestroyCount     metric.Int64ObservableCounter
		maxLifetimeDestroyCount metric.Int64ObservableCounter
		newConnsCount           metric.Int64ObservableCounter
		totalConns              metric.Int64ObservableUpDownCounter
		emptyAcquireWaitTime    metric.Int64ObservableCounter

		observeOptions []metric.ObserveOption

		dbStats     *pgxpool.Stat
		lastDBStats time.Time

		// lock prevents a race between batch observer and instrument registration.
		lock sync.Mutex
	)

	serverAddress := semconv.ServerAddress(db.Config().ConnConfig.Host)
	serverPort := semconv.ServerPort(int(db.Config().ConnConfig.Port))
	dbNamespace := semconv.DBNamespace(db.Config().ConnConfig.Database)
	poolName := fmt.Sprintf("%s:%d/%s", serverAddress.Value.AsString(), serverPort.Value.AsInt64(), dbNamespace.Value.AsString())
	dbClientConnectionPoolName := semconv.DBClientConnectionPoolName(poolName)

	lock.Lock()
	defer lock.Unlock()

	if acquireCount, err = meter.Int64ObservableCounter(
		pgxPoolAcquireCount,
		metric.WithDescription("Cumulative count of successful acquires from the pool."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolAcquireCount, err)
	}

	if acquireDuration, err = meter.Int64ObservableCounter(
		pgxPoolAcquireDuration,
		metric.WithDescription("Total duration of all successful acquires from the pool in nanoseconds."),
		metric.WithUnit("ns"),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolAcquireDuration, err)
	}

	if acquiredConns, err = meter.Int64ObservableUpDownCounter(
		pgxPoolAcquiredConnections,
		metric.WithDescription("Number of currently acquired connections in the pool."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolAcquiredConnections, err)
	}

	if cancelledAcquires, err = meter.Int64ObservableCounter(
		pgxPoolCancelledAcquires,
		metric.WithDescription("Cumulative count of acquires from the pool that were canceled by a context."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolCancelledAcquires, err)
	}

	if constructingConns, err = meter.Int64ObservableUpDownCounter(
		pgxPoolConstructingConnections,
		metric.WithUnit("ms"),
		metric.WithDescription("Number of connections with construction in progress in the pool."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolConstructingConnections, err)
	}

	if emptyAcquires, err = meter.Int64ObservableCounter(
		pgxPoolEmptyAcquire,
		metric.WithDescription("Cumulative count of successful acquires from the pool that waited for a resource to be released or constructed because the pool was empty."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolEmptyAcquire, err)
	}

	if idleConns, err = meter.Int64ObservableUpDownCounter(
		pgxPoolIdleConnections,
		metric.WithDescription("Number of currently idle connections in the pool."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolIdleConnections, err)
	}

	if maxConns, err = meter.Int64ObservableGauge(
		pgxPoolMaxConnections,
		metric.WithDescription("Maximum size of the pool."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolMaxConnections, err)
	}

	if maxIdleDestroyCount, err = meter.Int64ObservableCounter(
		pgxPoolMaxIdleDestroyCount,
		metric.WithDescription("Cumulative count of connections destroyed because they exceeded MaxConnectionsIdleTime."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolMaxIdleDestroyCount, err)
	}

	if maxLifetimeDestroyCount, err = meter.Int64ObservableCounter(
		pgxPoolMaxLifetimeDestroyCount,
		metric.WithDescription("Cumulative count of connections destroyed because they exceeded MaxConnectionsLifetime."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolMaxLifetimeDestroyCount, err)
	}

	if newConnsCount, err = meter.Int64ObservableCounter(
		pgxPoolNewConnectionsCount,
		metric.WithDescription("Cumulative count of new connections opened."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolNewConnectionsCount, err)
	}

	if totalConns, err = meter.Int64ObservableUpDownCounter(
		pgxPoolTotalConnections,
		metric.WithDescription("Total number of resources currently in the pool. The value is the sum of ConstructingConnections, AcquiredConnections, and IdleConnections."),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolTotalConnections, err)
	}

	if emptyAcquireWaitTime, err = meter.Int64ObservableCounter(
		pgxPoolEmptyAcquireWaitTime,
		metric.WithDescription("Total time waited for successful acquires from the pool for a resource to be released or constructed because the pool was empty."),
		metric.WithUnit("ns"),
	); err != nil {
		return fmt.Errorf("failed to create asynchronous metric: %s with error: %w", pgxPoolEmptyAcquireWaitTime, err)
	}

	attrs = append(attrs, []attribute.KeyValue{
		semconv.DBSystemPostgreSQL,
		dbClientConnectionPoolName,
	}...)

	observeOptions = []metric.ObserveOption{
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	}

	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			lock.Lock()
			defer lock.Unlock()

			now := time.Now()
			if now.Sub(lastDBStats) >= minimumReadDBStatsInterval {
				dbStats = db.Stat()
				lastDBStats = now
			}

			o.ObserveInt64(acquireCount, dbStats.AcquireCount(), observeOptions...)
			o.ObserveInt64(acquireDuration, dbStats.AcquireDuration().Nanoseconds(), observeOptions...)
			o.ObserveInt64(acquiredConns, int64(dbStats.AcquiredConns()), observeOptions...)
			o.ObserveInt64(cancelledAcquires, dbStats.CanceledAcquireCount(), observeOptions...)
			o.ObserveInt64(constructingConns, int64(dbStats.ConstructingConns()), observeOptions...)
			o.ObserveInt64(emptyAcquires, dbStats.EmptyAcquireCount(), observeOptions...)
			o.ObserveInt64(idleConns, int64(dbStats.IdleConns()), observeOptions...)
			o.ObserveInt64(maxConns, int64(dbStats.MaxConns()), observeOptions...)
			o.ObserveInt64(maxIdleDestroyCount, dbStats.MaxIdleDestroyCount(), observeOptions...)
			o.ObserveInt64(maxLifetimeDestroyCount, dbStats.MaxLifetimeDestroyCount(), observeOptions...)
			o.ObserveInt64(newConnsCount, dbStats.NewConnsCount(), observeOptions...)
			o.ObserveInt64(totalConns, int64(dbStats.TotalConns()), observeOptions...)
			o.ObserveInt64(emptyAcquireWaitTime, dbStats.EmptyAcquireWaitTime().Nanoseconds(), observeOptions...)

			return nil
		},
		acquireCount,
		acquireDuration,
		acquiredConns,
		cancelledAcquires,
		constructingConns,
		emptyAcquires,
		idleConns,
		maxConns,
		maxIdleDestroyCount,
		maxLifetimeDestroyCount,
		newConnsCount,
		totalConns,
		emptyAcquireWaitTime,
	)

	return err
}
