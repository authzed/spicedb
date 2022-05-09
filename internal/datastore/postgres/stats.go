package postgres

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"

	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	tableMetadata = "metadata"
	colUniqueID   = "unique_id"

	tablePGClass = "pg_class"
	colReltuples = "reltuples"
	colRelname   = "relname"
)

var (
	queryUniqueID          = psql.Select(colUniqueID).From(tableMetadata)
	queryEstimatedRowCount = psql.
				Select(colReltuples).
				From(tablePGClass).
				Where(sq.Eq{colRelname: tableTuple})
)

func (pgd *pgDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	idSQL, idArgs, err := queryUniqueID.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to generate query sql: %w", err)
	}

	rowCountSQL, rowCountArgs, err := queryEstimatedRowCount.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to prepare row count sql: %w", err)
	}

	nsQuery := readNamespace.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	var uniqueID string
	var nsDefs []*corev1.NamespaceDefinition
	var relCount int64
	if err := pgd.dbpool.BeginTxFunc(ctx, pgd.readTxOptions, func(tx pgx.Tx) error {
		if pgd.analyzeBeforeStatistics {
			if _, err := tx.Exec(ctx, fmt.Sprintf("ANALYZE %s", tableTuple)); err != nil {
				return fmt.Errorf("unable to analyze tuple table: %w", err)
			}
		}

		if err := tx.QueryRow(ctx, idSQL, idArgs...).Scan(&uniqueID); err != nil {
			return fmt.Errorf("unable to query unique ID: %w", err)
		}

		nsDefs, err = loadAllNamespaces(ctx, tx, nsQuery)
		if err != nil {
			return fmt.Errorf("unable to load namespaces: %w", err)
		}

		if err := tx.QueryRow(ctx, rowCountSQL, rowCountArgs...).Scan(&relCount); err != nil {
			return fmt.Errorf("unable to read relationship count: %w", err)
		}

		return nil
	}); err != nil {
		return datastore.Stats{}, err
	}

	// Sometimes relCount can be negative on postgres, truncate to 0
	var relCountUint uint64
	if relCount > 0 {
		relCountUint = uint64(relCount)
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
		EstimatedRelationshipCount: relCountUint,
	}, nil
}
