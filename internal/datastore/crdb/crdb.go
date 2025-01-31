package crdb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/IBM/pgxpoolprometheus"
	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"resenje.org/singleflight"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

var ParseRevisionString = revisions.RevisionParser(revisions.HybridLogicalClock)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	gcTTLRegex = regexp.MustCompile(`gc\.ttlseconds\s*=\s*([1-9][0-9]+)`)

	tracer = otel.Tracer("spicedb/internal/datastore/crdb")
)

const (
	Engine = "cockroachdb"

	errUnableToInstantiate = "unable to instantiate datastore"
	errRevision            = "unable to find revision: %w"

	querySelectNow            = "SELECT cluster_logical_timestamp()"
	queryTransactionNowPreV23 = querySelectNow
	queryTransactionNow       = "SHOW COMMIT TIMESTAMP"
	queryShowZoneConfig       = "SHOW ZONE CONFIGURATION FOR RANGE default;"

	spicedbTransactionKey = "$spicedb_transaction_key"
)

var livingTupleConstraints = []string{"pk_relation_tuple"}

func newCRDBDatastore(ctx context.Context, url string, options ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}

	includeQueryParametersInTraces := config.includeQueryParametersInTraces
	readPoolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}
	err = config.readPoolOpts.ConfigurePgx(readPoolConfig, includeQueryParametersInTraces)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}

	writePoolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}
	err = config.writePoolOpts.ConfigurePgx(writePoolConfig, includeQueryParametersInTraces)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}

	initCtx, initCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer initCancel()

	healthChecker, err := pool.NewNodeHealthChecker(url)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}

	// The initPool is a 1-connection pool that is only used for setup tasks.
	// The actual pools are not given the initCtx, since cancellation can
	// interfere with pool setup.
	initPoolConfig := readPoolConfig.Copy()
	initPoolConfig.MinConns = 1
	initPool, err := pool.NewRetryPool(initCtx, "init", initPoolConfig, healthChecker, config.maxRetries, config.connectRate)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}
	defer initPool.Close()

	var version crdbVersion
	if err := queryServerVersion(initCtx, initPool, &version); err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}

	changefeedQuery := queryChangefeed
	if version.Major < 22 {
		log.Info().Object("version", version).Msg("using changefeed query for CRDB version < 22")
		changefeedQuery = queryChangefeedPreV22
	}

	transactionNowQuery := queryTransactionNow
	if version.Major < 23 {
		log.Info().Object("version", version).Msg("using transaction now query for CRDB version < 23")
		transactionNowQuery = queryTransactionNowPreV23
	}

	clusterTTLNanos, err := readClusterTTLNanos(initCtx, initPool)
	if err != nil {
		return nil, fmt.Errorf("unable to read cluster gc window: %w", err)
	}

	gcWindowNanos := config.gcWindow.Nanoseconds()
	if clusterTTLNanos < gcWindowNanos {
		log.Warn().
			Int64("cockroach_cluster_gc_window_nanos", clusterTTLNanos).
			Int64("spicedb_gc_window_nanos", gcWindowNanos).
			Msg("configured CockroachDB cluster gc window is less than configured SpiceDB gc window, falling back to CRDB value - see https://spicedb.dev/d/crdb-gc-window-warning")
		config.gcWindow = time.Duration(clusterTTLNanos) * time.Nanosecond
	}

	keySetInit := newKeySet
	var keyer overlapKeyer
	switch config.overlapStrategy {
	case overlapStrategyStatic:
		if len(config.overlapKey) == 0 {
			return nil, fmt.Errorf("static tx overlap strategy specified without an overlap key")
		}
		keyer = appendStaticKey(config.overlapKey)
	case overlapStrategyPrefix:
		keyer = prefixKeyer
	case overlapStrategyRequest:
		// overlap keys are computed over requests and not data
		keyer = noOverlapKeyer
		keySetInit = overlapKeysFromContext
	case overlapStrategyInsecure:
		log.Warn().Str("strategy", overlapStrategyInsecure).
			Msg("running in this mode is only safe when replicas == nodes")
		keyer = noOverlapKeyer
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	headMigration, err := migrations.CRDBMigrations.HeadRevision()
	if err != nil {
		return nil, fmt.Errorf("invalid head migration found for cockroach: %w", err)
	}

	ds := &crdbDatastore{
		RemoteClockRevisions: revisions.NewRemoteClockRevisions(
			config.gcWindow,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		CommonDecoder:           revisions.CommonDecoder{Kind: revisions.HybridLogicalClock},
		MigrationValidator:      common.NewMigrationValidator(headMigration, config.allowedMigrations),
		dburl:                   url,
		watchBufferLength:       config.watchBufferLength,
		watchBufferWriteTimeout: config.watchBufferWriteTimeout,
		watchConnectTimeout:     config.watchConnectTimeout,
		writeOverlapKeyer:       keyer,
		overlapKeyInit:          keySetInit,
		beginChangefeedQuery:    changefeedQuery,
		transactionNowQuery:     transactionNowQuery,
		analyzeBeforeStatistics: config.analyzeBeforeStatistics,
		filterMaximumIDCount:    config.filterMaximumIDCount,
		supportsIntegrity:       config.withIntegrity,
		gcWindow:                config.gcWindow,
		schema:                  *schema.Schema(config.columnOptimizationOption, config.withIntegrity, config.expirationDisabled),
	}
	ds.RemoteClockRevisions.SetNowFunc(ds.headRevisionInternal)

	// this ctx and cancel is tied to the lifetime of the datastore
	ds.ctx, ds.cancel = context.WithCancel(context.Background())
	ds.writePool, err = pool.NewRetryPool(ds.ctx, "write", writePoolConfig, healthChecker, config.maxRetries, config.connectRate)
	if err != nil {
		ds.cancel()
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}
	ds.readPool, err = pool.NewRetryPool(ds.ctx, "read", readPoolConfig, healthChecker, config.maxRetries, config.connectRate)
	if err != nil {
		ds.cancel()
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, url)
	}

	if config.enablePrometheusStats {
		if err := prometheus.Register(pgxpoolprometheus.NewCollector(ds.writePool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "write",
		})); err != nil {
			ds.cancel()
			return nil, err
		}

		if err := prometheus.Register(pgxpoolprometheus.NewCollector(ds.readPool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "read",
		})); err != nil {
			ds.cancel()
			return nil, err
		}
	}

	// TODO: this (and the GC startup that it's based on for mysql/pg) should
	// be removed and have the lifetimes tied to server start/stop.

	// Start goroutines for pruning
	if config.enableConnectionBalancing {
		log.Ctx(initCtx).Info().Msg("starting cockroach connection balancer")
		ds.pruneGroup, ds.ctx = errgroup.WithContext(ds.ctx)
		writePoolBalancer := pool.NewNodeConnectionBalancer(ds.writePool, healthChecker, 5*time.Second)
		readPoolBalancer := pool.NewNodeConnectionBalancer(ds.readPool, healthChecker, 5*time.Second)
		ds.pruneGroup.Go(func() error {
			writePoolBalancer.Prune(ds.ctx)
			return nil
		})
		ds.pruneGroup.Go(func() error {
			readPoolBalancer.Prune(ds.ctx)
			return nil
		})
		ds.pruneGroup.Go(func() error {
			healthChecker.Poll(ds.ctx, 5*time.Second)
			return nil
		})
	}

	return ds, nil
}

// NewCRDBDatastore initializes a SpiceDB datastore that uses a CockroachDB
// database while leveraging its AOST functionality.
func NewCRDBDatastore(ctx context.Context, url string, options ...Option) (datastore.Datastore, error) {
	ds, err := newCRDBDatastore(ctx, url, options...)
	if err != nil {
		return nil, err
	}
	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

type crdbDatastore struct {
	*revisions.RemoteClockRevisions
	revisions.CommonDecoder
	*common.MigrationValidator

	dburl                   string
	readPool, writePool     *pool.RetryPool
	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration
	watchConnectTimeout     time.Duration
	writeOverlapKeyer       overlapKeyer
	overlapKeyInit          func(ctx context.Context) keySet
	analyzeBeforeStatistics bool
	gcWindow                time.Duration
	schema                  common.SchemaInformation

	beginChangefeedQuery string
	transactionNowQuery  string

	featureGroup singleflight.Group[string, *datastore.Features]

	pruneGroup           *errgroup.Group
	ctx                  context.Context
	cancel               context.CancelFunc
	filterMaximumIDCount uint16
	supportsIntegrity    bool
}

func (cds *crdbDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	executor := common.QueryRelationshipsExecutor{
		Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(cds.readPool, cds),
	}
	return &crdbReader{
		schema:               cds.schema,
		query:                cds.readPool,
		executor:             executor,
		keyer:                noOverlapKeyer,
		overlapKeySet:        nil,
		filterMaximumIDCount: cds.filterMaximumIDCount,
		withIntegrity:        cds.supportsIntegrity,
		atSpecificRevision:   rev.String(),
	}
}

func (cds *crdbDatastore) MetricsID() (string, error) {
	return common.MetricsIDFromURL(cds.dburl)
}

func (cds *crdbDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	var commitTimestamp datastore.Revision

	config := options.NewRWTOptionsWithOptions(opts...)
	if config.DisableRetries {
		ctx = context.WithValue(ctx, pool.CtxDisableRetries, true)
	}

	err := cds.writePool.BeginFunc(ctx, func(tx pgx.Tx) error {
		querier := pgxcommon.QuerierFuncsFor(tx)
		executor := common.QueryRelationshipsExecutor{
			Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(querier, cds),
		}

		// Write metadata onto the transaction.
		metadata := config.Metadata.AsMap()

		// Mark the transaction as coming from SpiceDB. See the comment in watch.go
		// for why this is necessary.
		metadata[spicedbTransactionKey] = true

		expiresAt := time.Now().Add(cds.gcWindow).Add(1 * time.Minute)
		insertTransactionMetadata := psql.Insert(schema.TableTransactionMetadata).
			Columns(schema.ColExpiresAt, schema.ColMetadata).
			Values(expiresAt, metadata)

		sql, args, err := insertTransactionMetadata.ToSql()
		if err != nil {
			return fmt.Errorf("error building metadata insert: %w", err)
		}

		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("error writing metadata: %w", err)
		}

		reader := &crdbReader{
			schema:               cds.schema,
			query:                querier,
			executor:             executor,
			keyer:                cds.writeOverlapKeyer,
			overlapKeySet:        cds.overlapKeyInit(ctx),
			filterMaximumIDCount: cds.filterMaximumIDCount,
			withIntegrity:        cds.supportsIntegrity,
			atSpecificRevision:   "", // No AS OF SYSTEM TIME for writes
		}

		rwt := &crdbReadWriteTXN{
			reader,
			tx,
			0,
		}

		if err := f(ctx, rwt); err != nil {
			return err
		}

		// Touching the transaction key happens last so that the "write intent" for
		// the transaction as a whole lands in a range for the affected tuples.
		for k := range rwt.overlapKeySet {
			if _, err := tx.Exec(ctx, queryTouchTransaction, k); err != nil {
				return fmt.Errorf("error writing overlapping keys: %w", err)
			}
		}

		var cerr error
		commitTimestamp, cerr = cds.readTransactionCommitRev(ctx, querier)
		if cerr != nil {
			return fmt.Errorf("error getting commit timestamp: %w", cerr)
		}
		return nil
	})
	if err != nil {
		return datastore.NoRevision, wrapError(err)
	}

	return commitTimestamp, nil
}

func wrapError(err error) error {
	// If a unique constraint violation is returned, then its likely that the cause
	// was an existing relationship.
	if cerr := pgxcommon.ConvertToWriteConstraintError(livingTupleConstraints, err); cerr != nil {
		return cerr
	}
	return err
}

func (cds *crdbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	currentRevision, err := migrations.NewCRDBDriver(cds.dburl)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	defer currentRevision.Close(ctx)

	version, err := currentRevision.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	if state := cds.MigrationValidator.MigrationReadyState(version); !state.IsReady {
		return state, nil
	}

	readMin := cds.readPool.MinConns()
	if readMin > 0 {
		readMin--
	}
	writeMin := cds.writePool.MinConns()
	if writeMin > 0 {
		writeMin--
	}
	writeTotal, err := safecast.ToUint32(cds.writePool.Stat().TotalConns())
	if err != nil {
		return datastore.ReadyState{}, spiceerrors.MustBugf("could not cast writeTotal to uint32: %v", err)
	}
	readTotal, err := safecast.ToUint32(cds.readPool.Stat().TotalConns())
	if err != nil {
		return datastore.ReadyState{}, spiceerrors.MustBugf("could not cast readTotal to uint32: %v", err)
	}
	if writeTotal < writeMin || readTotal < readMin {
		return datastore.ReadyState{
			Message: fmt.Sprintf(
				"spicedb does not have the required minimum connection count to the datastore. Read: %d/%d, Write: %d/%d",
				readTotal,
				readMin,
				writeTotal,
				writeMin,
			),
			IsReady: false,
		}, nil
	}
	return datastore.ReadyState{IsReady: true}, nil
}

func (cds *crdbDatastore) Close() error {
	cds.cancel()
	cds.readPool.Close()
	cds.writePool.Close()
	return nil
}

func (cds *crdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return cds.headRevisionInternal(ctx)
}

func (cds *crdbDatastore) headRevisionInternal(ctx context.Context) (datastore.Revision, error) {
	var hlcNow datastore.Revision

	var fnErr error
	hlcNow, fnErr = readCRDBNow(ctx, cds.readPool)
	if fnErr != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, fnErr)
	}

	return hlcNow, fnErr
}

func (cds *crdbDatastore) OfflineFeatures() (*datastore.Features, error) {
	if cds.supportsIntegrity {
		return &datastore.Features{
			IntegrityData: datastore.Feature{
				Status: datastore.FeatureSupported,
			},
			ContinuousCheckpointing: datastore.Feature{
				Status: datastore.FeatureSupported,
			},
			WatchEmitsImmediately: datastore.Feature{
				Status: datastore.FeatureSupported,
			},
		}, nil
	}

	return &datastore.Features{
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
	}, nil
}

func (cds *crdbDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	features, _, err := cds.featureGroup.Do(ctx, "", func(ictx context.Context) (*datastore.Features, error) {
		return cds.features(ictx)
	})
	return features, err
}

func (cds *crdbDatastore) features(ctx context.Context) (*datastore.Features, error) {
	features := datastore.Features{
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
	}
	if cds.supportsIntegrity {
		features.IntegrityData.Status = datastore.FeatureSupported
	}

	head, err := cds.HeadRevision(ctx)
	if err != nil {
		return nil, err
	}

	// Start a changefeed with an invalid value. If we get back an invalid value error (SQLSTATE 22023)
	// then we know that the datastore supports watch. If we get back any other error, then we know that
	// the datastore does not support watch emits or there is a permissions issue.
	_ = cds.writePool.ExecFunc(ctx, func(ctx context.Context, tag pgconn.CommandTag, err error) error {
		if err == nil {
			return spiceerrors.MustBugf("expected an error, but got none")
		}

		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			if pgerr.Code == "22023" {
				features.Watch.Status = datastore.FeatureSupported
				return nil
			}
		}

		features.Watch.Status = datastore.FeatureUnsupported
		features.Watch.Reason = fmt.Sprintf("Range feeds must be enabled in CockroachDB and the user must have permission to create them in order to enable the Watch API: %s", err.Error())
		return nil
	}, fmt.Sprintf(cds.beginChangefeedQuery, cds.schema.RelationshipTableName, head, "-1s"))

	return &features, nil
}

func (cds *crdbDatastore) readTransactionCommitRev(ctx context.Context, reader pgxcommon.DBFuncQuerier) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "readTransactionCommitRev")
	defer span.End()

	var hlcNow decimal.Decimal
	if err := reader.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&hlcNow)
	}, cds.transactionNowQuery); err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to read timestamp: %w", err)
	}

	return revisions.NewForHLC(hlcNow)
}

func readCRDBNow(ctx context.Context, reader pgxcommon.DBFuncQuerier) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "readCRDBNow")
	defer span.End()

	var hlcNow decimal.Decimal
	if err := reader.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&hlcNow)
	}, querySelectNow); err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to read timestamp: %w", err)
	}

	return revisions.NewForHLC(hlcNow)
}

func readClusterTTLNanos(ctx context.Context, conn pgxcommon.DBFuncQuerier) (int64, error) {
	var target, configSQL string

	if err := conn.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&target, &configSQL)
	}, queryShowZoneConfig); err != nil {
		return 0, err
	}

	groups := gcTTLRegex.FindStringSubmatch(configSQL)
	if groups == nil || len(groups) != 2 {
		return 0, fmt.Errorf("CRDB zone config unexpected format")
	}

	gcSeconds, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return 0, err
	}

	return gcSeconds * 1_000_000_000, nil
}
