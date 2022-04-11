package mysql

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
)

func (mds *Datastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	query, args, err := sb.Select("table_rows").From("INFORMATION_SCHEMA.TABLES").Where(squirrel.Eq{"table_name": mds.driver.RelationTuple()}).ToSql()
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
		UniqueID:                   uuid.NewString(), // FIXME actually persist a uniqueID in the DB
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(nsDefs),
		EstimatedRelationshipCount: count,
	}, nil
}
