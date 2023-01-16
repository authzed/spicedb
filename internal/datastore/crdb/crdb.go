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
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
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

	querySelectNow          = "SELECT cluster_logical_timestamp()"
	queryShowZoneConfig     = "SHOW ZONE CONFIGURATION FOR RANGE default;"
	querySetTransactionTime = "SET TRANSACTION AS OF SYSTEM TIME %s"

	livingTupleConstraint = "pk_relation_tuple"
)

func newCRDBDatastore(url string, options ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	configurePool(config, poolConfig)

	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(initCtx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	var version crdbVersion
	if err := queryServerVersion(initCtx, pool, &version); err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	changefeedQuery := queryChangefeed
	if version.Major < 22 {
		log.Info().Object("version", version).Msg("using changefeed query for CRDB version < 22")
		changefeedQuery = queryChangefeedPreV22
	}

	if config.enablePrometheusStats {
		collector := pgxpoolprometheus.NewCollector(pool, map[string]string{"db_name": "spicedb"})
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
		if err := common.RegisterGCMetrics(); err != nil {
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
	}

	clusterTTLNanos, err := readClusterTTLNanos(initCtx, pool)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	gcWindowNanos := config.gcWindow.Nanoseconds()
	if clusterTTLNanos < gcWindowNanos {
		return nil, fmt.Errorf(
			errUnableToInstantiate,
			fmt.Errorf("cluster gc window is less than requested gc window %d < %d",
				clusterTTLNanos,
				gcWindowNanos,
			),
		)
	}

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
	case overlapStrategyInsecure:
		log.Warn().Str("strategy", overlapStrategyInsecure).
			Msg("running in this mode is only safe when replicas == nodes")
		keyer = noOverlapKeyer
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	ds := &crdbDatastore{
		revisions.NewRemoteClockRevisions(
			config.gcWindow,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		revision.DecimalDecoder{},
		url,
		pool,
		config.watchBufferLength,
		keyer,
		config.splitAtUsersetCount,
		executeWithMaxRetries(config.maxRetries),
		config.disableStats,
		changefeedQuery,
	}

	ds.RemoteClockRevisions.SetNowFunc(ds.headRevisionInternal)

	return ds, nil
}

// NewCRDBDatastore initializes a SpiceDB datastore that uses a CockroachDB
// database while leveraging its AOST functionality.
func NewCRDBDatastore(url string, options ...Option) (datastore.Datastore, error) {
	ds, err := newCRDBDatastore(url, options...)
	if err != nil {
		return nil, err
	}
	return proxy.NewSeparatingContextDatastoreProxy(ds), nil
}

func configurePool(config crdbOptions, pgxConfig *pgxpool.Config) {
	if config.maxOpenConns != nil {
		pgxConfig.MaxConns = int32(*config.maxOpenConns)
	}

	if config.minOpenConns != nil {
		pgxConfig.MinConns = int32(*config.minOpenConns)
	}

	if pgxConfig.MaxConns > 0 && pgxConfig.MinConns > 0 && pgxConfig.MaxConns < pgxConfig.MinConns {
		log.Warn().Int32("max-connections", pgxConfig.MaxConns).Int32("min-connections", pgxConfig.MinConns).Msg("maximum number of connections configured is less than minimum number of connections; minimum will be used")
	}

	if config.connMaxIdleTime != nil {
		pgxConfig.MaxConnIdleTime = *config.connMaxIdleTime
	}

	if config.connMaxLifetime != nil {
		pgxConfig.MaxConnLifetime = *config.connMaxLifetime
	}

	if config.connHealthCheckInterval != nil {
		pgxConfig.HealthCheckPeriod = *config.connHealthCheckInterval
	}

	pgxcommon.ConfigurePGXLogger(pgxConfig.ConnConfig)
}

type crdbDatastore struct {
	*revisions.RemoteClockRevisions
	revision.DecimalDecoder

	dburl             string
	pool              *pgxpool.Pool
	watchBufferLength uint16
	writeOverlapKeyer overlapKeyer
	usersetBatchSize  uint16
	execute           executeTxRetryFunc
	disableStats      bool

	beginChangefeedQuery string
}

func (cds *crdbDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	createTxFunc := func(ctx context.Context) (pgx.Tx, common.TxCleanupFunc, error) {
		tx, err := cds.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, nil, err
		}

		cleanup := func(ctx context.Context) {
			if err := tx.Rollback(ctx); err != nil {
				log.Ctx(ctx).Err(err).Msg("error rolling back transaction")
			}
		}

		setTxTime := fmt.Sprintf(querySetTransactionTime, rev)
		if _, err := tx.Exec(ctx, setTxTime); err != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg(
					"error rolling back transaction after failing to set transaction time",
				)
			}
			return nil, nil, err
		}

		return tx, cleanup, nil
	}

	querySplitter := common.TupleQuerySplitter{
		Executor:         pgxcommon.NewPGXExecutor(createTxFunc),
		UsersetBatchSize: cds.usersetBatchSize,
	}

	return &crdbReader{createTxFunc, querySplitter, noOverlapKeyer, nil, cds.execute}
}

func noCleanup(context.Context) {}

func (cds *crdbDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
) (datastore.Revision, error) {
	var commitTimestamp revision.Decimal
	if err := cds.execute(ctx, func(ctx context.Context) error {
		return cds.pool.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			longLivedTx := func(context.Context) (pgx.Tx, common.TxCleanupFunc, error) {
				return tx, noCleanup, nil
			}

			querySplitter := common.TupleQuerySplitter{
				Executor:         pgxcommon.NewPGXExecutor(longLivedTx),
				UsersetBatchSize: cds.usersetBatchSize,
			}

			rwt := &crdbReadWriteTXN{
				&crdbReader{
					longLivedTx,
					querySplitter,
					cds.writeOverlapKeyer,
					make(keySet),
					executeOnce,
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
				commitTimestamp, err = readCRDBNow(ctx, tx)
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
	}); err != nil {
		return datastore.NoRevision, err
	}

	return commitTimestamp, nil
}

func (cds *crdbDatastore) IsReady(ctx context.Context) (bool, error) {
	headMigration, err := migrations.CRDBMigrations.HeadRevision()
	if err != nil {
		return false, fmt.Errorf("invalid head migration found for postgres: %w", err)
	}

	currentRevision, err := migrations.NewCRDBDriver(cds.dburl)
	if err != nil {
		return false, err
	}
	defer currentRevision.Close(ctx)

	version, err := currentRevision.Version(ctx)
	if err != nil {
		return false, err
	}

	return version == headMigration, nil
}

func (cds *crdbDatastore) Close() error {
	cds.pool.Close()
	return nil
}

func (cds *crdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return cds.headRevisionInternal(ctx)
}

func (cds *crdbDatastore) headRevisionInternal(ctx context.Context) (revision.Decimal, error) {
	var hlcNow revision.Decimal
	err := cds.execute(ctx, func(ctx context.Context) error {
		return cds.pool.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
			var fnErr error
			hlcNow, fnErr = readCRDBNow(ctx, tx)
			if fnErr != nil {
				hlcNow = revision.NoRevision
				return fmt.Errorf(errRevision, fnErr)
			}
			return nil
		})
	})

	return hlcNow, err
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
	_, err = cds.pool.Exec(streamCtx, fmt.Sprintf(cds.beginChangefeedQuery, tableTuple, head))
	if err != nil && errors.Is(err, context.Canceled) {
		features.Watch.Enabled = true
		features.Watch.Reason = ""
	} else if err != nil {
		features.Watch.Enabled = false
		features.Watch.Reason = fmt.Sprintf("Range feeds must be enabled in CockroachDB and the user must have permission to create them in order to enable the Watch API: %s", err.Error())
	}
	<-streamCtx.Done()

	return &features, nil
}

func readCRDBNow(ctx context.Context, tx pgx.Tx) (revision.Decimal, error) {
	ctx, span := tracer.Start(ctx, "readCRDBNow")
	defer span.End()

	var hlcNow decimal.Decimal
	if err := tx.QueryRow(ctx, querySelectNow).Scan(&hlcNow); err != nil {
		return revision.NoRevision, fmt.Errorf("unable to read timestamp: %w", err)
	}

	return revision.NewFromDecimal(hlcNow), nil
}

func readClusterTTLNanos(ctx context.Context, conn *pgxpool.Pool) (int64, error) {
	var target, configSQL string
	if err := conn.
		QueryRow(ctx, queryShowZoneConfig).
		Scan(&target, &configSQL); err != nil {
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
