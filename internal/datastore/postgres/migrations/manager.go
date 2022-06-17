package migrations

import (
	"github.com/authzed/spicedb/pkg/migrate"

	"github.com/jackc/pgx/v4"
)

var (
	noNonatomicMigration migrate.MigrationFunc[*pgx.Conn]
	noTxMigration        migrate.TxMigrationFunc[pgx.Tx]
)

// DatabaseMigrations implements a migration manager for the Postgres Driver.
var DatabaseMigrations = migrate.NewManager[*AlembicPostgresDriver, *pgx.Conn, pgx.Tx]()
