package crdb

// TODO: add tracing
// TODO: make sure that DB connections don't get canceled when an error occurs (separate context)

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

	"github.com/authzed/spicedb/internal/datastore"
)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	gcTTLRegex = regexp.MustCompile(`gc\.ttlseconds\s*=\s*([1-9][0-9]+)`)
)

const (
	tableNamespace = "namespace_config"
	tableTuple     = "relation_tuple"

	colNamespace        = "namespace"
	colConfig           = "serialized_config"
	colTimestamp        = "timestamp"
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

func NewCRDBDatastore(url string, options ...CRDBOption) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	quantizationNanos := config.revisionQuantization.Nanoseconds()

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

	return &crdbDatastore{
		conn:              conn,
		watchBufferLength: config.watchBufferLength,
		quantizationNanos: quantizationNanos,
		gcWindowNanos:     gcWindowNanos,
	}, nil
}

type crdbDatastore struct {
	conn              *pgxpool.Pool
	watchBufferLength uint16
	quantizationNanos int64
	gcWindowNanos     int64
}

func (cds *crdbDatastore) Revision(ctx context.Context) (datastore.Revision, error) {
	// Get the current system revision and round it down to the nearest quantization
	nowHLC, err := cds.SyncRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	now := nowHLC.IntPart()

	quantized := now - (now % cds.quantizationNanos)

	return decimal.NewFromInt(quantized), nil
}

func (cds *crdbDatastore) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	// Return the current system time
	tx, err := cds.conn.Begin(ctx)
	defer tx.Rollback(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	return readCRDBNow(ctx, tx)
}

func (cds *crdbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	// Make sure the system time indicated is within the software GC window

	now, err := cds.SyncRevision(ctx)
	if err != nil {
		return err
	}

	nowNanos := now.IntPart()
	revisionNanos := revision.IntPart()

	if revisionNanos < (nowNanos - cds.gcWindowNanos) {
		fmt.Printf("revision was too old %d < %d - %d\n", revisionNanos, nowNanos, cds.gcWindowNanos)
	}

	if revisionNanos > nowNanos {
		fmt.Println("revision in the future ")
	}

	if (revisionNanos < (nowNanos - cds.gcWindowNanos)) || (revisionNanos > nowNanos) {
		return datastore.ErrInvalidRevision
	}

	return nil
}

func readCRDBNow(ctx context.Context, tx pgx.Tx) (decimal.Decimal, error) {
	var hlcNow decimal.Decimal
	if err := tx.QueryRow(ctx, querySelectNow).Scan(&hlcNow); err != nil {
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
