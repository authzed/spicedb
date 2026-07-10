package spanner

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/datastore/common"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
)

func init() {
	datastorecfg.RegisterEngine(Engine, newDatastoreFromConfig)
}

func newDatastoreFromConfig(ctx context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	if len(opts.ReadReplicaURIs) > 0 {
		return nil, errors.New("read replicas are not supported for the Spanner datastore engine")
	}

	metricsOption := DatastoreMetricsOption(opts.SpannerDatastoreMetricsOption)
	if !opts.EnableDatastoreMetrics {
		metricsOption = DatastoreMetricsOptionNone
	}

	watchChangeBufferMaximumSize, err := common.WatchBufferSize(opts.WatchChangeBufferMaximumSize)
	if err != nil {
		return nil, err
	}

	return NewSpannerDatastore(
		ctx,
		opts.URI,
		FollowerReadDelay(opts.FollowerReadDelay),
		RevisionQuantization(opts.RevisionQuantization),
		MaxRevisionStalenessPercent(opts.MaxRevisionStalenessPercent),
		//nolint:staticcheck // the deprecated credentials options remain supported until removal
		CredentialsFile(opts.SpannerCredentialsFile),
		//nolint:staticcheck // the deprecated credentials options remain supported until removal
		CredentialsJSON(opts.SpannerCredentialsJSON),
		WatchBufferLength(opts.WatchBufferLength),
		WatchChangeBufferMaximumSize(watchChangeBufferMaximumSize),
		WatchBufferWriteTimeout(opts.WatchBufferWriteTimeout),
		EmulatorHost(opts.SpannerEmulatorHost),
		DisableStats(opts.DisableStats),
		WithDatastoreMetricsOption(metricsOption),
		ReadConnsMaxOpen(opts.ReadConnPool.MaxOpenConns),
		WriteConnsMaxOpen(opts.WriteConnPool.MaxOpenConns),
		MigrationPhase(opts.MigrationPhase),
		AllowedMigrations(opts.AllowedMigrations),
		FilterMaximumIDCount(opts.FilterMaximumIDCount),
		WithColumnOptimization(opts.ExperimentalColumnOptimization),
		WithWatchDisabled(opts.DisableWatchSupport),
	)
}
