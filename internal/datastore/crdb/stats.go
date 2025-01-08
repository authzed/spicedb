package crdb

import (
	"context"
	"fmt"
	"slices"

	"github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	tableMetadata = "metadata"
	colUniqueID   = "unique_id"
)

var (
	queryReadUniqueID = psql.Select(colUniqueID).From(tableMetadata)
	uniqueID          string
)

func (cds *crdbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	if len(uniqueID) == 0 {
		sql, args, err := queryReadUniqueID.ToSql()
		if err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to prepare unique ID sql: %w", err)
		}
		if err := cds.readPool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
			return row.Scan(&uniqueID)
		}, sql, args...); err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to query unique ID: %w", err)
		}
	}

	var nsDefs []datastore.RevisionedNamespace
	if err := cds.readPool.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
		if err != nil {
			return fmt.Errorf("unable to read namespaces: %w", err)
		}
		nsDefs, _, err = loadAllNamespaces(ctx, pgxcommon.QuerierFuncsFor(tx), func(sb squirrel.SelectBuilder, tableName string) squirrel.SelectBuilder {
			return sb.From(tableName)
		})
		if err != nil {
			return fmt.Errorf("unable to read namespaces: %w", err)
		}
		return nil
	}); err != nil {
		return datastore.Stats{}, err
	}

	if cds.analyzeBeforeStatistics {
		if err := cds.readPool.BeginTxFunc(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "ANALYZE "+cds.schema.RelationshipTableName); err != nil {
				return fmt.Errorf("unable to analyze tuple table: %w", err)
			}

			return nil
		}); err != nil {
			return datastore.Stats{}, err
		}
	}

	var estimatedRelCount uint64
	if err := cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		hasRows := false

		for rows.Next() {
			hasRows = true
			values, err := rows.Values()
			if err != nil {
				log.Warn().Err(err).Msg("unable to read statistics")
				return nil
			}

			// Find the row whose column_names contains the expected columns for the
			// full relationship.
			isFullRelationshipRow := false
			for index, fd := range rows.FieldDescriptions() {
				if fd.Name != "column_names" {
					continue
				}

				columnNames, ok := values[index].([]any)
				if !ok {
					log.Warn().Msg("unable to read column names")
					return nil
				}

				if slices.Contains(columnNames, "namespace") &&
					slices.Contains(columnNames, "object_id") &&
					slices.Contains(columnNames, "relation") &&
					slices.Contains(columnNames, "userset_namespace") &&
					slices.Contains(columnNames, "userset_object_id") &&
					slices.Contains(columnNames, "userset_relation") {
					isFullRelationshipRow = true
					break
				}
			}

			if !isFullRelationshipRow {
				continue
			}

			// Read the estimated relationship count.
			for index, fd := range rows.FieldDescriptions() {
				if fd.Name != "row_count" {
					continue
				}

				rowCount, ok := values[index].(int64)
				if !ok {
					log.Warn().Msg("unable to read row count")
					return nil
				}

				uintRowCount, err := safecast.ToUint64(rowCount)
				if err != nil {
					return spiceerrors.MustBugf("row count was negative: %v", err)
				}
				estimatedRelCount = uintRowCount
				return nil
			}
		}

		log.Warn().Bool("has-rows", hasRows).Msg("unable to find row count in statistics query result")
		return nil
	}, "SHOW STATISTICS FOR TABLE "+cds.schema.RelationshipTableName); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to query unique estimated row count: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		EstimatedRelationshipCount: estimatedRelCount,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
	}, nil
}
