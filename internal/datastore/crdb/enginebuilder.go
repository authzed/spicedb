package crdb

import (
	"context"
	"errors"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
)

func init() {
	datastorecfg.RegisterEngine(Engine, newDatastoreFromConfig)
	migration.RegisterMigratableEngine(Engine, migrations.CRDBMigrations, newMigrationDriverFromConfig, "add-schema-tables")
}

func newMigrationDriverFromConfig(ctx context.Context, cfg *migration.Config) (*migrations.CRDBDriver, error) {
	return migrations.NewCRDBDriver(ctx, cfg.DatastoreURI)
}

func newDatastoreFromConfig(ctx context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	if len(opts.ReadReplicaURIs) > 0 {
		return nil, errors.New("read replicas are not supported for the CockroachDB datastore engine")
	}

	maxRetries, err := safecast.Convert[uint8](opts.MaxRetries)
	if err != nil {
		return nil, errors.New("max-retries could not be cast to uint8")
	}

	watchChangeBufferMaximumSize, err := common.WatchBufferSize(opts.WatchChangeBufferMaximumSize)
	if err != nil {
		return nil, err
	}

	return NewCRDBDatastore(
		ctx,
		opts.URI,
		GCWindow(opts.GCWindow),
		RevisionQuantization(opts.RevisionQuantization),
		MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		ReadConnsMaxOpen(opts.ReadConnPool.MaxOpenConns),
		ReadConnsMinOpen(opts.ReadConnPool.MinOpenConns),
		ReadConnMaxIdleTime(opts.ReadConnPool.MaxIdleTime),
		ReadConnMaxLifetime(opts.ReadConnPool.MaxLifetime),
		ReadConnMaxLifetimeJitter(opts.ReadConnPool.MaxLifetimeJitter),
		ReadConnHealthCheckInterval(opts.ReadConnPool.HealthCheckInterval),
		ReadConnPingTimeout(opts.ReadConnPool.PingTimeout),
		WithAcquireTimeout(opts.WriteAcquisitionTimeout),
		WriteConnsMaxOpen(opts.WriteConnPool.MaxOpenConns),
		WriteConnsMinOpen(opts.WriteConnPool.MinOpenConns),
		WriteConnMaxIdleTime(opts.WriteConnPool.MaxIdleTime),
		WriteConnMaxLifetime(opts.WriteConnPool.MaxLifetime),
		WriteConnMaxLifetimeJitter(opts.WriteConnPool.MaxLifetimeJitter),
		WriteConnHealthCheckInterval(opts.WriteConnPool.HealthCheckInterval),
		WriteConnPingTimeout(opts.WriteConnPool.PingTimeout),
		FollowerReadDelay(opts.FollowerReadDelay),
		MaxRetries(maxRetries),
		OverlapKey(opts.OverlapKey),
		OverlapStrategy(opts.OverlapStrategy),
		WatchBufferLength(opts.WatchBufferLength),
		WatchChangeBufferMaximumSize(watchChangeBufferMaximumSize),
		WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		WatchConnectTimeout(opts.WatchConnectTimeout),
		WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		WithEnableConnectionBalancing(opts.EnableConnectionBalancing),
		ConnectRate(opts.ConnectRate),
		FilterMaximumIDCount(opts.FilterMaximumIDCount),
		WithIntegrity(opts.RelationshipIntegrityEnabled),
		AllowedMigrations(opts.AllowedMigrations),
		WithColumnOptimization(opts.ExperimentalColumnOptimization),
		IncludeQueryParametersInTraces(opts.IncludeQueryParametersInTraces),
		WithWatchDisabled(opts.DisableWatchSupport),
	)
}
