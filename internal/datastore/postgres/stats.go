package postgres

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
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
				Where(sq.Eq{colRelname: schema.TableTuple})
)

func (pgd *pgDatastore) datastoreUniqueID(ctx context.Context) (string, error) {
	idSQL, idArgs, err := queryUniqueID.ToSql()
	if err != nil {
		return "", fmt.Errorf("unable to generate query sql: %w", err)
	}

	var uniqueID string
	return uniqueID, pgx.BeginTxFunc(ctx, pgd.readPool, pgd.readTxOptions, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, idSQL, idArgs...).Scan(&uniqueID)
	})
}

func (pgd *pgDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	idSQL, idArgs, err := queryUniqueID.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to generate query sql: %w", err)
	}

	rowCountSQL, rowCountArgs, err := queryEstimatedRowCount.ToSql()
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to prepare row count sql: %w", err)
	}

	aliveFilter := func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})
	}

	var uniqueID string
	var nsDefs []datastore.RevisionedNamespace
	var relCount float64
	if err := pgx.BeginTxFunc(ctx, pgd.readPool, pgd.readTxOptions, func(tx pgx.Tx) error {
		if pgd.analyzeBeforeStatistics {
			if _, err := tx.Exec(ctx, "ANALYZE "+schema.TableTuple); err != nil {
				return fmt.Errorf("unable to analyze tuple table: %w", err)
			}
		}

		if err := tx.QueryRow(ctx, idSQL, idArgs...).Scan(&uniqueID); err != nil {
			return fmt.Errorf("unable to query unique ID: %w", err)
		}

		nsDefsWithRevisions, err := loadAllNamespaces(ctx, pgxcommon.QuerierFuncsFor(tx), aliveFilter)
		if err != nil {
			return fmt.Errorf("unable to load namespaces: %w", err)
		}

		nsDefs = nsDefsWithRevisions

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
