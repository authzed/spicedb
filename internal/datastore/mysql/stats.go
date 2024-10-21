package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	informationSchemaTableRowsColumn = "table_rows"
	informationSchemaTablesTable     = "INFORMATION_SCHEMA.TABLES"
	informationSchemaTableNameColumn = "table_name"

	metadataIDColumn       = "id"
	metadataUniqueIDColumn = "unique_id"
)

func (mds *Datastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	if mds.analyzeBeforeStats {
		_, err := mds.db.ExecContext(ctx, "ANALYZE TABLE "+mds.driver.RelationTuple())
		if err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to run ANALYZE TABLE: %w", err)
		}
	}

	uniqueID, err := mds.getUniqueID(ctx)
	if err != nil {
		return datastore.Stats{}, err
	}

	query, args, err := sb.
		Select(informationSchemaTableRowsColumn).
		From(informationSchemaTablesTable).
		Where(squirrel.Eq{informationSchemaTableNameColumn: mds.driver.RelationTuple()}).
		ToSql()
	if err != nil {
		return datastore.Stats{}, err
	}

	var count sql.NullInt64
	err = mds.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return datastore.Stats{}, err
	}

	if !count.Valid || count.Int64 == 0 {
		// If we get a count of zero, its possible the information schema table has not yet
		// been updated, so we use a slower count(*) call.
		query, args, err := mds.QueryBuilder.CountRelsQuery.ToSql()
		if err != nil {
			return datastore.Stats{}, err
		}
		err = mds.db.QueryRowContext(ctx, query, args...).Scan(&count)
		if err != nil {
			return datastore.Stats{}, err
		}
	}

	nsQuery := mds.ReadNamespaceQuery.Where(squirrel.Eq{colDeletedTxn: liveDeletedTxnID})

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return datastore.Stats{}, err
	}
	defer common.LogOnError(ctx, tx.Rollback)

	nsDefs, err := loadAllNamespaces(ctx, tx, nsQuery)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to load namespaces: %w", err)
	}

	uintCount, err := safecast.ToUint64(count.Int64)
	if err != nil {
		return datastore.Stats{}, spiceerrors.MustBugf("could not cast count to uint64: %v", err)
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
		EstimatedRelationshipCount: uintCount,
	}, nil
}

func (mds *Datastore) getUniqueID(ctx context.Context) (string, error) {
	sql, args, err := sb.Select(metadataUniqueIDColumn).From(mds.driver.Metadata()).ToSql()
	if err != nil {
		return "", fmt.Errorf("unable to generate query sql: %w", err)
	}

	var uniqueID string
	if err := mds.db.QueryRowContext(ctx, sql, args...).Scan(&uniqueID); err != nil {
		return "", fmt.Errorf("unable to query unique ID: %w", err)
	}

	return uniqueID, nil
}
