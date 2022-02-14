package crdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/alecthomas/units"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	gcTTLRegex = regexp.MustCompile(`gc\.ttlseconds\s*=\s*([1-9][0-9]+)`)

	tracer = otel.Tracer("spicedb/internal/datastore/crdb")
)

const (
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

	gcWindowNanos := config.gcWindow.Nanoseconds()
	clusterTTLNanos, err := readClusterTTLNanos(conn)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

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

	followerReadDelayNanos := config.followerReadDelay.Nanoseconds()

	return &crdbDatastore{
		dburl:                     url,
		conn:                      conn,
		watchBufferLength:         config.watchBufferLength,
		quantizationNanos:         config.revisionQuantization.Nanoseconds(),
		maxRevisionStaleness:      maxRevisionStaleness,
		gcWindowNanos:             gcWindowNanos,
		followerReadDelayNanos:    followerReadDelayNanos,
		splitAtEstimatedQuerySize: config.splitAtEstimatedQuerySize,
		execute:                   executeWithMaxRetries(config.maxRetries),
		overlapKeyer:              keyer,
	}, nil
}

type crdbDatastore struct {
	dburl                     string
	conn                      *pgxpool.Pool
	watchBufferLength         uint16
	quantizationNanos         int64
	maxRevisionStaleness      time.Duration
	gcWindowNanos             int64
	followerReadDelayNanos    int64
	splitAtEstimatedQuerySize units.Base2Bytes
	execute                   executeTxRetryFunc
	overlapKeyer              overlapKeyer

	lastQuantizedRevision decimal.Decimal
	revisionValidThrough  time.Time
}

func (cds *crdbDatastore) IsReady(ctx context.Context) (bool, error) {
	headMigration, err := migrations.Manager.HeadRevision()
	if err != nil {
		return false, fmt.Errorf("invalid head migration found for postgres: %w", err)
	}

	currentRevision, err := migrations.NewCRDBDriver(cds.dburl)
	if err != nil {
		return false, err
	}
	defer currentRevision.Dispose()

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

func (cds *crdbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "OptimizedRevision")
	defer span.End()

	localNow := time.Now()
	if localNow.Before(cds.revisionValidThrough) {
		log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", cds.revisionValidThrough).Msg("returning cached revision")
		return cds.lastQuantizedRevision, nil
	}

	log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", cds.revisionValidThrough).Msg("computing new revision")

	nowHLC, err := cds.HeadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	// Round the revision down to the nearest quantization
	// Apply a delay to enable follower reads: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	crdbNow := nowHLC.IntPart() - cds.followerReadDelayNanos
	quantized := crdbNow
	if cds.quantizationNanos > 0 {
		quantized -= (crdbNow % cds.quantizationNanos)
	}
	log.Ctx(ctx).Debug().Int64("readSkew", cds.followerReadDelayNanos).Int64("totalSkew", nowHLC.IntPart()-quantized).Msg("revision skews")

	validForNanos := (quantized + cds.quantizationNanos) - crdbNow

	cds.revisionValidThrough = localNow.
		Add(time.Duration(validForNanos) * time.Nanosecond).
		Add(cds.maxRevisionStaleness)
	log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", cds.revisionValidThrough).Int64("validForNanos", validForNanos).Msg("setting valid through")
	cds.lastQuantizedRevision = decimal.NewFromInt(quantized)

	return cds.lastQuantizedRevision, nil
}

func (cds *crdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "HeadRevision")
	defer span.End()

	tx, err := cds.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}
	defer tx.Rollback(ctx)

	hlcNow, err := readCRDBNow(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	return hlcNow, nil
}

func (cds *crdbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	// Make sure the system time indicated is within the software GC window
	now, err := cds.HeadRevision(ctx)
	if err != nil {
		return err
	}

	nowNanos := now.IntPart()
	revisionNanos := revision.IntPart()

	staleRevision := revisionNanos < (nowNanos - cds.gcWindowNanos)
	if staleRevision {
		log.Ctx(ctx).Debug().Stringer("now", now).Stringer("revision", revision).Msg("stale revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	futureRevision := revisionNanos > nowNanos
	if futureRevision {
		log.Ctx(ctx).Debug().Stringer("now", now).Stringer("revision", revision).Msg("future revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	return nil
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
