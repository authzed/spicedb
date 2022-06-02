package crdb

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	tableMetadata = "metadata"
	colUniqueID   = "unique_id"

	tableCounters = "relationship_estimate_counters"
	colID         = "id"
	colCount      = "count"
)

var (
	queryReadUniqueID         = psql.Select(colUniqueID).From(tableMetadata)
	queryRelationshipEstimate = fmt.Sprintf("SELECT COALESCE(SUM(%s), 0) FROM %s", colCount, tableCounters)

	upsertCounterQuery = psql.Insert(tableCounters).Columns(
		colID,
		colCount,
	).Suffix(fmt.Sprintf("ON CONFLICT (%[1]s) DO UPDATE SET %[2]s = %[3]s.%[2]s + EXCLUDED.%[2]s RETURNING cluster_logical_timestamp()", colID, colCount, tableCounters))
)

func (cds *crdbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	sql, args, err := queryReadUniqueID.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to prepare unique ID sql: %w", err)
	}

	var uniqueID string
	var nsDefs []*corev1.NamespaceDefinition
	var relCount uint64
	if err := cds.pool.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, sql, args...).Scan(&uniqueID); err != nil {
			return fmt.Errorf("unable to query unique ID: %w", err)
		}

		if err := tx.QueryRow(ctx, queryRelationshipEstimate).Scan(&relCount); err != nil {
			return fmt.Errorf("unable to read relationship count: %w", err)
		}

		nsDefs, err = loadAllNamespaces(ctx, tx)
		if err != nil {
			return fmt.Errorf("unable to read namespaces: %w", err)
		}

		return nil
	}); err != nil {
		return datastore.Stats{}, err
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		EstimatedRelationshipCount: relCount,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
	}, nil
}

func updateCounter(ctx context.Context, tx pgx.Tx, change int64) (datastore.Revision, error) {
	counterID := make([]byte, 2)
	_, err := rand.Read(counterID)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to select random counter: %w", err)
	}

	sql, args, err := upsertCounterQuery.Values(counterID, change).ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to prepare upsert counter sql: %w", err)
	}

	var timestamp decimal.Decimal
	if err := tx.QueryRow(ctx, sql, args...).Scan(&timestamp); err != nil {
		return datastore.NoRevision, fmt.Errorf("unable to executed upsert counter query: %w", err)
	}

	return timestamp, nil
}
