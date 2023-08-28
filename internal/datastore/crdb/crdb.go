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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	gcTTLRegex = regexp.MustCompile(`gc\.ttlseconds\s*=\s*([1-9][0-9]+)`)

	tracer = otel.Tracer("spicedb/internal/datastore/crdb")
)

const (
	Engine            = "cockroachdb"
	tableNamespace    = "namespace_config"
	tableTuple        = "relation_tuple"
	tableTransactions = "transactions"
	tableCaveat       = "caveat"

	colNamespace         = "namespace"
	colConfig            = "serialized_config"
	colTimestamp         = "timestamp"
	colTransactionKey    = "key"
	colObjectID          = "object_id"
	colRelation          = "relation"
	colUsersetNamespace  = "userset_namespace"
	colUsersetObjectID   = "userset_object_id"
	colUsersetRelation   = "userset_relation"
	colCaveatName        = "name"
	colCaveatDefinition  = "definition"
	colCaveatContextName = "caveat_name"
	colCaveatContext     = "caveat_context"

	errUnableToInstantiate = "unable to instantiate datastore: %w"
	errRevision            = "unable to find revision: %w"

	querySelectNow      = "SELECT cluster_logical_timestamp()"
	queryShowZoneConfig = "SHOW ZONE CONFIGURATION FOR RANGE default;"

	livingTupleConstraint = "pk_relation_tuple"
)

func newCRDBDatastore(url string, options ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	readPoolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	config.readPoolOpts.ConfigurePgx(readPoolConfig)

	writePoolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	config.writePoolOpts.ConfigurePgx(writePoolConfig)

	initCtx, initCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer initCancel()

	healthChecker, err := pool.NewNodeHealthChecker(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// The initPool is a 1-connection pool that is only used for setup tasks.
	// The actual pools are not given the initCtx, since cancellation can
	// interfere with pool setup.
	initPoolConfig := readPoolConfig.Copy()
	initPoolConfig.MinConns = 1
	initPool, err := pool.NewRetryPool(initCtx, "init", initPoolConfig, healthChecker, config.maxRetries, config.connectRate)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	defer initPool.Close()

	var version crdbVersion
	if err := queryServerVersion(initCtx, initPool, &version); err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	changefeedQuery := queryChangefeed
	if version.Major < 22 {
		log.Info().Object("version", version).Msg("using changefeed query for CRDB version < 22")
		changefeedQuery = queryChangefeedPreV22
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
			return nil, fmt.Errorf(
				errUnableToInstantiate,
				fmt.Errorf("static tx overlap strategy specified without an overlap key"),
			)
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

	ds := &crdbDatastore{
		RemoteClockRevisions: revisions.NewRemoteClockRevisions(
			config.gcWindow,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		DecimalDecoder:       revision.DecimalDecoder{},
		dburl:                url,
		watchBufferLength:    config.watchBufferLength,
		writeOverlapKeyer:    keyer,
		overlapKeyInit:       keySetInit,
		disableStats:         config.disableStats,
		beginChangefeedQuery: changefeedQuery,
	}
	ds.RemoteClockRevisions.SetNowFunc(ds.headRevisionInternal)

	// this ctx and cancel is tied to the lifetime of the datastore
	ds.ctx, ds.cancel = context.WithCancel(context.Background())
	ds.writePool, err = pool.NewRetryPool(ds.ctx, "write", writePoolConfig, healthChecker, config.maxRetries, config.connectRate)
	if err != nil {
		ds.cancel()
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	ds.readPool, err = pool.NewRetryPool(ds.ctx, "read", readPoolConfig, healthChecker, config.maxRetries, config.connectRate)
	if err != nil {
		ds.cancel()
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if config.enablePrometheusStats {
		if err := prometheus.Register(pgxpoolprometheus.NewCollector(ds.writePool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "write",
		})); err != nil {
			ds.cancel()
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}

		if err := prometheus.Register(pgxpoolprometheus.NewCollector(ds.readPool, map[string]string{
			"db_name":    "spicedb",
			"pool_usage": "read",
		})); err != nil {
			ds.cancel()
			return nil, fmt.Errorf(errUnableToInstantiate, err)
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
func NewCRDBDatastore(url string, options ...Option) (datastore.Datastore, error) {
	ds, err := newCRDBDatastore(url, options...)
	if err != nil {
		return nil, err
	}
	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

type crdbDatastore struct {
	*revisions.RemoteClockRevisions
	revision.DecimalDecoder

	dburl               string
	readPool, writePool *pool.RetryPool
	watchBufferLength   uint16
	writeOverlapKeyer   overlapKeyer
	overlapKeyInit      func(ctx context.Context) keySet
	disableStats        bool

	beginChangefeedQuery string

	pruneGroup *errgroup.Group
	ctx        context.Context
	cancel     context.CancelFunc
}

func (cds *crdbDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	executor := common.QueryExecutor{
		Executor: pgxcommon.NewPGXExecutor(cds.readPool),
	}

	fromBuilder := func(query sq.SelectBuilder, fromStr string) sq.SelectBuilder {
		return query.From(fromStr + " AS OF SYSTEM TIME " + rev.String())
	}

	return &crdbReader{cds.readPool, executor, noOverlapKeyer, nil, fromBuilder}
}

func (cds *crdbDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	var commitTimestamp revision.Decimal

	config := options.NewRWTOptionsWithOptions(opts...)
	if config.DisableRetries {
		ctx = context.WithValue(ctx, pool.CtxDisableRetries, true)
	}

	err := cds.writePool.BeginFunc(ctx, func(tx pgx.Tx) error {
		querier := pgxcommon.QuerierFuncsFor(tx)
		executor := common.QueryExecutor{
			Executor: pgxcommon.NewPGXExecutor(querier),
		}

		rwt := &crdbReadWriteTXN{
			&crdbReader{
				querier,
				executor,
				cds.writeOverlapKeyer,
				cds.overlapKeyInit(ctx),
				func(query sq.SelectBuilder, fromStr string) sq.SelectBuilder {
					return query.From(fromStr)
				},
			},
			tx,
			0,
		}

		if err := f(rwt); err != nil {
			return err
		}

		// Touching the transaction key happens last so that the "write intent" for
		// the transaction as a whole lands in a range for the affected tuples.
		for k := range rwt.overlapKeySet {
			if _, err := tx.Exec(ctx, queryTouchTransaction, k); err != nil {
				return fmt.Errorf("error writing overlapping keys: %w", err)
			}
		}

		if cds.disableStats {
			var err error
			commitTimestamp, err = readCRDBNow(ctx, querier)
			if err != nil {
				return fmt.Errorf("error getting commit timestamp: %w", err)
			}
			return nil
		}

		var err error
		commitTimestamp, err = updateCounter(ctx, tx, rwt.relCountChange)
		if err != nil {
			return fmt.Errorf("error updating relationship counter: %w", err)
		}

		return nil
	})
	if err != nil {
		return datastore.NoRevision, err
	}

	return commitTimestamp, nil
}

func (cds *crdbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	headMigration, err := migrations.CRDBMigrations.HeadRevision()
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("invalid head migration found for cockroach: %w", err)
	}

	currentRevision, err := migrations.NewCRDBDriver(cds.dburl)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	defer currentRevision.Close(ctx)

	version, err := currentRevision.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	if version != headMigration {
		return datastore.ReadyState{
			Message: fmt.Sprintf(
				"datastore is not migrated: currently at revision `%s`, but requires `%s`. Please run `spicedb migrate`.",
				version,
				headMigration,
			),
			IsReady: false,
		}, nil
	}

	readMin := cds.readPool.MinConns()
	if readMin > 0 {
		readMin--
	}
	writeMin := cds.writePool.MinConns()
	if writeMin > 0 {
		writeMin--
	}
	writeTotal := uint32(cds.writePool.Stat().TotalConns())
	readTotal := uint32(cds.readPool.Stat().TotalConns())
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

func (cds *crdbDatastore) headRevisionInternal(ctx context.Context) (revision.Decimal, error) {
	var hlcNow revision.Decimal

	var fnErr error
	hlcNow, fnErr = readCRDBNow(ctx, cds.readPool)
	if fnErr != nil {
		return revision.NoRevision, fmt.Errorf(errRevision, fnErr)
	}

	return hlcNow, fnErr
}

func (cds *crdbDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	var features datastore.Features

	head, err := cds.HeadRevision(ctx)
	if err != nil {
		return nil, err
	}

	// streams don't return at all if they succeed, so the only way to know
	// it was created successfully is to wait a bit and then cancel
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(1*time.Second, cancel)

	_ = cds.writePool.ExecFunc(streamCtx, func(ctx context.Context, tag pgconn.CommandTag, err error) error {
		if err != nil && errors.Is(err, context.Canceled) {
			features.Watch.Enabled = true
			features.Watch.Reason = ""
		} else if err != nil {
			features.Watch.Enabled = false
			features.Watch.Reason = fmt.Sprintf("Range feeds must be enabled in CockroachDB and the user must have permission to create them in order to enable the Watch API: %s", err.Error())
		}
		return nil
	}, fmt.Sprintf(cds.beginChangefeedQuery, tableTuple, head))

	<-streamCtx.Done()

	return &features, nil
}

func readCRDBNow(ctx context.Context, reader pgxcommon.DBFuncQuerier) (revision.Decimal, error) {
	ctx, span := tracer.Start(ctx, "readCRDBNow")
	defer span.End()

	var hlcNow decimal.Decimal
	if err := reader.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&hlcNow)
	}, querySelectNow); err != nil {
		return revision.NoRevision, fmt.Errorf("unable to read timestamp: %w", err)
	}

	return revision.NewFromDecimal(hlcNow), nil
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

func revisionFromTimestamp(t time.Time) revision.Decimal {
	return revision.NewFromDecimal(decimal.NewFromInt(t.UnixNano()))
}
