package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	dropStatsTable = `DROP TABLE relationship_estimate_counters;`
)

func init() {
	err := CRDBMigrations.Register("remove-stats-table", "add-caveats", removeStatsTable, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func removeStatsTable(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, dropStatsTable); err != nil {
		return err
	}
	return nil
}
