package mysql

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
)

const (
	informationSchemaTableRowsColumn = "table_rows"
	informationSchemaTablesTable     = "INFORMATION_SCHEMA.TABLES"
	informationSchemaTableNameColumn = "table_name"

	analyzeTableQuery = "ANALYZE TABLE %s"
)

func (mds *Datastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	if mds.analyzeBeforeStats {
		_, err := mds.db.ExecContext(ctx, fmt.Sprintf(analyzeTableQuery, mds.driver.RelationTuple()))
		if err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to run ANALYZE TABLE: %w", err)
		}
	}

	query, args, err := sb.
		Select(informationSchemaTableRowsColumn).
		From(informationSchemaTablesTable).
		Where(squirrel.Eq{informationSchemaTableNameColumn: mds.driver.RelationTuple()}).
		ToSql()
	if err != nil {
		return datastore.Stats{}, err
	}
	var count uint64
	err = mds.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return datastore.Stats{}, err
	}

	nsQuery := mds.ReadNamespaceQuery.Where(squirrel.Eq{colDeletedTxn: liveDeletedTxnID})

	nsDefs, err := loadAllNamespaces(ctx, mds.db, nsQuery)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to load namespaces: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   uuid.NewString(), // TODO (@vroldanbet) actually persist a uniqueID in the DB
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
		EstimatedRelationshipCount: count,
	}, nil
}
