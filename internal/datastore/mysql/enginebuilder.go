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

	// SetLogger assigns an unsynchronized package-level variable in the driver, and
	// that same variable is read by ParseDSN every time a connection is configured.
	// Calling it while building a driver or a datastore is therefore a data race
	// against every other goroutine opening a MySQL connection at that moment, which
	// is why this belongs here and must not be moved back into a constructor: init
	// runs once, before any of those goroutines exist.
	//
	// The logger is always the same package-level logger, so there is nothing a
	// caller could vary; taking its address also means the driver keeps logging
	// through whatever logger the process later installs.
	//
	// SetLogger only fails on a nil logger, which the address of a package-level
	// variable can never be, so a failure here means the driver's contract changed
	// underneath us and there is no sensible way to continue with an unconfigured
	// driver.
	if err := sqlDriver.SetLogger(&log.Logger); err != nil {
		panic(fmt.Errorf("unable to set logging to mysql driver: %w", err))
	}
}

func newMigrationDriverFromConfig(ctx context.Context, cfg *migration.Config) (*migrations.MySQLDriver, error) {
	credentialsProvider, err := cfg.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
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
