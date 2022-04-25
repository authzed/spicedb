package crdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
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

	colNamespace        = "namespace"
	colConfig           = "serialized_config"
	colTimestamp        = "timestamp"
	colTransactionKey   = "key"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"

	errUnableToInstantiate = "unable to instantiate datastore: %w"
	errRevision            = "unable to find revision: %w"

	querySelectNow          = "SELECT cluster_logical_timestamp()"
	queryReturningTimestamp = "RETURNING cluster_logical_timestamp()"
	queryShowZoneConfig     = "SHOW ZONE CONFIGURATION FOR RANGE default;"
)

// NewCRDBDatastore initializes a SpiceDB datastore that uses a CockroachDB
// database while leveraging its AOST functionality.
func NewCRDBDatastore(url string, options ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if config.maxOpenConns != nil {
		poolConfig.MaxConns = int32(*config.maxOpenConns)
	}

	if config.minOpenConns != nil {
		poolConfig.MinConns = int32(*config.minOpenConns)
	}

	if config.connMaxIdleTime != nil {
		poolConfig.MaxConnIdleTime = *config.connMaxIdleTime
	}

	if config.connMaxLifetime != nil {
		poolConfig.MaxConnLifetime = *config.connMaxLifetime
	}

	poolConfig.ConnConfig.Logger = zerologadapter.NewLogger(log.Logger)

	conn, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	clusterTTLNanos, err := readClusterTTLNanos(conn)
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

	querySplitter := common.TupleQuerySplitter{
		Executor:         common.NewPGXExecutor(conn, prepareTransaction),
		UsersetBatchSize: int(config.splitAtUsersetCount),
	}

	ds := &crdbDatastore{
		revisions.NewRemoteClockRevisions(
			config.gcWindow,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		url,
		conn,
		config.watchBufferLength,
		querySplitter,
		executeWithMaxRetries(config.maxRetries),
		keyer,
	}

	ds.RemoteClockRevisions.SetNowFunc(ds.HeadRevision)

	return ds, nil
}

type crdbDatastore struct {
	*revisions.RemoteClockRevisions

	dburl             string
	conn              *pgxpool.Pool
	watchBufferLength uint16
	querySplitter     common.TupleQuerySplitter
	execute           executeTxRetryFunc
	overlapKeyer      overlapKeyer
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
	defer currentRevision.Close()

	version, err := currentRevision.Version()
	if err != nil {
		return false, err
	}

	return version == headMigration, nil
}

func (cds *crdbDatastore) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	return fmt.Sprintf("%s@%s", namespaceName, revision), nil
}

func (cds *crdbDatastore) Close() error {
	cds.conn.Close()
	return nil
}

func (cds *crdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "HeadRevision")
	defer span.End()

	var hlcNow datastore.Revision
	err := cds.execute(ctx, cds.conn, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var fnErr error
		hlcNow, fnErr = readCRDBNow(ctx, tx)
		if fnErr != nil {
			hlcNow = datastore.NoRevision
			return fmt.Errorf(errRevision, fnErr)
		}
		return nil
	})

	return hlcNow, err
}

func (cds *crdbDatastore) AddOverlapKey(keySet map[string]struct{}, namespace string) {
	cds.overlapKeyer.AddKey(keySet, namespace)
}

func readCRDBNow(ctx context.Context, tx pgx.Tx) (decimal.Decimal, error) {
	ctx, span := tracer.Start(ctx, "readCRDBNow")
	defer span.End()

	var hlcNow decimal.Decimal
	if err := tx.QueryRow(
		datastore.SeparateContextWithTracing(ctx),
		querySelectNow,
	).Scan(&hlcNow); err != nil {
		return decimal.Decimal{}, fmt.Errorf("unable to read timestamp: %w", err)
	}

	return hlcNow, nil
}

func readClusterTTLNanos(conn *pgxpool.Pool) (int64, error) {
	var target, configSQL string
	if err := conn.
		QueryRow(context.Background(), queryShowZoneConfig).
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

func revisionFromTimestamp(t time.Time) datastore.Revision {
	return decimal.NewFromInt(t.UnixNano())
}
