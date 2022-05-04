package postgres

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/pkg/datastore"
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
	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to establish transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if pgd.analyzeBeforeStatistics {
		if _, err := tx.Exec(ctx, fmt.Sprintf("ANALYZE %s", tableTuple)); err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to analyze tuple table: %w", err)
		}
	}

	idSQL, idArgs, err := queryUniqueID.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to generate query sql: %w", err)
	}
	defer tx.Rollback(ctx)

	var uniqueID string
	if err := tx.QueryRow(ctx, idSQL, idArgs...).Scan(&uniqueID); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to query unique ID: %w", err)
	}

	nsQuery := readNamespace.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	nsDefs, err := loadAllNamespaces(ctx, tx, nsQuery)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to load namespaces: %w", err)
	}

	rowCountSQL, rowCountArgs, err := queryEstimatedRowCount.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to prepare row count sql: %w", err)
	}

	var relCount int64
	if err := tx.QueryRow(ctx, rowCountSQL, rowCountArgs...).Scan(&relCount); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read relationship count: %w", err)
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
