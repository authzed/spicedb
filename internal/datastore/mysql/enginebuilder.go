package mysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/ccoveille/go-safecast/v2"
	sqlDriver "github.com/go-sql-driver/mysql"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	log "github.com/authzed/spicedb/internal/logging"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
)

func init() {
	datastorecfg.RegisterEngine(Engine, newDatastoreFromConfig)
	migration.RegisterMigratableEngine(Engine, migrations.Manager, newMigrationDriverFromConfig, "add_schema_tables")
}

func newMigrationDriverFromConfig(ctx context.Context, cfg *migration.Config) (*migrations.MySQLDriver, error) {
	credentialsProvider, err := cfg.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
	}

	// Do this outside NewMySQLDriverFromDSN to avoid races on MySQL datastore tests
	if err := sqlDriver.SetLogger(&log.Logger); err != nil {
		return nil, fmt.Errorf("unable to set logging to mysql driver: %w", err)
	}

	return migrations.NewMySQLDriverFromDSN(cfg.DatastoreURI, cfg.MySQLTablePrefix, credentialsProvider)
}

func newDatastoreFromConfig(ctx context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	primary, err := newPrimaryDatastoreFromConfig(ctx, opts)
	if err != nil {
		return nil, err
	}

	if len(opts.ReadReplicaURIs) > datastorecfg.MaxReplicaCount {
		return nil, fmt.Errorf("too many read replicas, max is %d", datastorecfg.MaxReplicaCount)
	}

	replicas := make([]datastore.ReadOnlyDatastore, 0, len(opts.ReadReplicaURIs))
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

	return proxy.NewCheckingReplicatedDatastore(primary, replicas...)
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
		TablePrefix(opts.TablePrefix),
		MaxRetries(maxRetries),
		OverrideLockWaitTimeout(1),
		WithEnablePrometheusStats(opts.EnableDatastoreMetrics),
		WatchBufferLength(opts.WatchBufferLength),
		WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		WatchChangeBufferMaximumSize(watchChangeBufferMaximumSize),
		MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		RevisionQuantization(opts.RevisionQuantization),
		FilterMaximumIDCount(opts.FilterMaximumIDCount),
		AllowedMigrations(opts.AllowedMigrations),
		WithColumnOptimization(opts.ExperimentalColumnOptimization),
	}, nil
}

func newReplicaDatastoreFromConfig(ctx context.Context, replicaIndex uint32, replicaURI string, opts datastorecfg.Config) (datastore.ReadOnlyDatastore, error) {
	mysqlOpts := []Option{ //nolint: prealloc  // we're not concerned about perf here
		MaxOpenConns(opts.ReadReplicaConnPool.MaxOpenConns),
		ConnMaxIdleTime(opts.ReadReplicaConnPool.MaxIdleTime),
		ConnMaxLifetime(opts.ReadReplicaConnPool.MaxLifetime),
		CredentialsProviderName(opts.ReadReplicaCredentialsProviderName),
	}

	commonOptions, err := commonDatastoreOptionsFromConfig(opts)
	if err != nil {
		return nil, err
	}
	mysqlOpts = append(mysqlOpts, commonOptions...)
	return NewReadOnlyMySQLDatastore(ctx, replicaURI, replicaIndex, mysqlOpts...)
}

func newPrimaryDatastoreFromConfig(ctx context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	mysqlOpts := []Option{ //nolint: prealloc  // we're not concerned about perf here
		GCInterval(opts.GCInterval),
		GCWindow(opts.GCWindow),
		GCInterval(opts.GCInterval),
		GCEnabled(!opts.ReadOnly),
		GCMaxOperationTime(opts.GCMaxOperationTime),
		MaxOpenConns(opts.ReadConnPool.MaxOpenConns),
		ConnMaxIdleTime(opts.ReadConnPool.MaxIdleTime),
		ConnMaxLifetime(opts.ReadConnPool.MaxLifetime),
		WithWatchDisabled(opts.DisableWatchSupport),
		CredentialsProviderName(opts.CredentialsProviderName),
		FollowerReadDelay(opts.FollowerReadDelay),
	}

	commonOptions, err := commonDatastoreOptionsFromConfig(opts)
	if err != nil {
		return nil, err
	}
	mysqlOpts = append(mysqlOpts, commonOptions...)
	return NewMySQLDatastore(ctx, opts.URI, mysqlOpts...)
}
