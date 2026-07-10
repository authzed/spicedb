package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
)

func init() {
	datastorecfg.RegisterEngine(Engine, newDatastoreFromConfig)
	migration.RegisterMigratableEngine(Engine, migrations.DatabaseMigrations, newMigrationDriverFromConfig, "add-schema-tables")
}

func newMigrationDriverFromConfig(ctx context.Context, cfg *migration.Config) (*migrations.AlembicPostgresDriver, error) {
	credentialsProvider, err := cfg.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
	}
	return migrations.NewAlembicPostgresDriver(ctx, cfg.DatastoreURI, credentialsProvider, false)
}

func newDatastoreFromConfig(ctx context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	primary, err := newPrimaryDatastoreFromConfig(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary datastore: %w", err)
	}

	if len(opts.ReadReplicaURIs) > datastorecfg.MaxReplicaCount {
		return nil, fmt.Errorf("too many read replicas, max is %d", datastorecfg.MaxReplicaCount)
	}

	replicas := make([]datastore.StrictReadDatastore, 0, len(opts.ReadReplicaURIs))
	for index, replicaURI := range opts.ReadReplicaURIs {
		uintIndex, err := safecast.Convert[uint32](index)
		if err != nil {
			return nil, errors.New("too many replicas")
		}
		replica, err := newReplicaDatastoreFromConfig(ctx, uintIndex, replicaURI, opts)
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}

	return proxy.NewStrictReplicatedDatastore(primary, replicas...)
}

func commonDatastoreOptionsFromConfig(opts datastorecfg.Config) ([]Option, error) {
	maxRetries, err := safecast.Convert[uint8](opts.MaxRetries)
	if err != nil {
		return nil, errors.New("max-retries could not be cast to uint8")
	}

	watchChangeBufferMaximumSize, err := common.WatchBufferSize(opts.WatchChangeBufferMaximumSize)
	if err != nil {
		return nil, err
	}

	return []Option{
		EnableTracing(),
		WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		MaxRetries(maxRetries),
		FilterMaximumIDCount(opts.FilterMaximumIDCount),
		WithColumnOptimization(opts.ExperimentalColumnOptimization),
		WatchChangeBufferMaximumSize(watchChangeBufferMaximumSize),
		IncludeQueryParametersInTraces(opts.IncludeQueryParametersInTraces),
	}, nil
}

func newReplicaDatastoreFromConfig(ctx context.Context, replicaIndex uint32, replicaURI string, opts datastorecfg.Config) (datastore.StrictReadDatastore, error) {
	pgOpts := []Option{ //nolint: prealloc  // we're not worried about perf here
		CredentialsProviderName(opts.ReadReplicaCredentialsProviderName),
		ReadConnsMaxOpen(opts.ReadReplicaConnPool.MaxOpenConns),
		ReadConnsMinOpen(opts.ReadReplicaConnPool.MinOpenConns),
		ReadConnMaxIdleTime(opts.ReadReplicaConnPool.MaxIdleTime),
		ReadConnMaxLifetime(opts.ReadReplicaConnPool.MaxLifetime),
		ReadConnMaxLifetimeJitter(opts.ReadReplicaConnPool.MaxLifetimeJitter),
		ReadConnHealthCheckInterval(opts.ReadReplicaConnPool.HealthCheckInterval),
		ReadConnPingTimeout(opts.ReadReplicaConnPool.PingTimeout),
		ReadStrictMode( /* strict read mode is required for Postgres read replicas */ true),
	}

	commonOptions, err := commonDatastoreOptionsFromConfig(opts)
	if err != nil {
		return nil, err
	}
	pgOpts = append(pgOpts, commonOptions...)
	return NewReadOnlyPostgresDatastore(ctx, replicaURI, replicaIndex, pgOpts...)
}

func newPrimaryDatastoreFromConfig(ctx context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	pgOpts := []Option{ //nolint: prealloc  // we're not worried about perf here
		CredentialsProviderName(opts.CredentialsProviderName),
		GCWindow(opts.GCWindow),
		GCEnabled(!opts.ReadOnly),
		RevisionQuantization(opts.RevisionQuantization),
		MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		FollowerReadDelay(opts.FollowerReadDelay),
		ReadConnsMaxOpen(opts.ReadConnPool.MaxOpenConns),
		ReadConnsMinOpen(opts.ReadConnPool.MinOpenConns),
		ReadConnMaxIdleTime(opts.ReadConnPool.MaxIdleTime),
		ReadConnMaxLifetime(opts.ReadConnPool.MaxLifetime),
		ReadConnMaxLifetimeJitter(opts.ReadConnPool.MaxLifetimeJitter),
		ReadConnHealthCheckInterval(opts.ReadConnPool.HealthCheckInterval),
		ReadConnPingTimeout(opts.ReadConnPool.PingTimeout),
		WriteConnsMaxOpen(opts.WriteConnPool.MaxOpenConns),
		WriteConnsMinOpen(opts.WriteConnPool.MinOpenConns),
		WriteConnMaxIdleTime(opts.WriteConnPool.MaxIdleTime),
		WriteConnMaxLifetime(opts.WriteConnPool.MaxLifetime),
		WriteConnMaxLifetimeJitter(opts.ReadConnPool.MaxLifetimeJitter),
		WriteConnHealthCheckInterval(opts.WriteConnPool.HealthCheckInterval),
		WriteConnPingTimeout(opts.WriteConnPool.PingTimeout),
		GCInterval(opts.GCInterval),
		GCMaxOperationTime(opts.GCMaxOperationTime),
		WatchBufferLength(opts.WatchBufferLength),
		WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		WithWatchDisabled(opts.DisableWatchSupport),
		MigrationPhase(opts.MigrationPhase),
		AllowedMigrations(opts.AllowedMigrations),
		WithRevisionHeartbeat(opts.EnableRevisionHeartbeat),
		WithRelaxedIsolationLevel(opts.RelaxedIsolationLevel),
	}

	commonOptions, err := commonDatastoreOptionsFromConfig(opts)
	if err != nil {
		return nil, err
	}
	pgOpts = append(pgOpts, commonOptions...)
	return NewPostgresDatastore(ctx, opts.URI, pgOpts...)
}
