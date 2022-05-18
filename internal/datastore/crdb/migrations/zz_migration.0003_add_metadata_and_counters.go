package migrations

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
)

const (
	createMetadataTable = `CREATE TABLE metadata (
    unique_id VARCHAR PRIMARY KEY
);`

	createCounters = `CREATE TABLE relationship_estimate_counters (
	id BYTES PRIMARY KEY,
	count INT NOT NULL DEFAULT 0
);`

	insertUniqueID = `INSERT INTO metadata (unique_id) VALUES ($1);`
)

func init() {
	if err := CRDBMigrations.Register("add-metadata-and-counters", "add-transactions-table", func(apd *CRDBDriver) error {
		ctx := context.Background()

		return apd.db.BeginFunc(ctx, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, createMetadataTable); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, createCounters); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx, insertUniqueID, uuid.NewString()); err != nil {
				return err
			}

			psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
			prefillCounterQuery := psql.Insert("relationship_estimate_counters").Columns(
				"id",
				"count",
			)
			for i := byte(0); i < 127; i++ {
				for j := byte(0); j < 127; j++ {
					prefillCounterQuery = prefillCounterQuery.Values([]byte{i, j}, 0)
				}
			}

			sql, args, err := prefillCounterQuery.ToSql()
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return err
			}

			prefillCounterQuery = psql.Insert("relationship_estimate_counters").Columns(
				"id",
				"count",
			)
			for i := byte(128); i < 255; i++ {
				for j := byte(128); j < 255; j++ {
					prefillCounterQuery = prefillCounterQuery.Values([]byte{i, j}, 0)
				}
			}

			sql, args, err = prefillCounterQuery.ToSql()
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return err
			}

			return nil
		})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
