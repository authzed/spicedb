package migrations

import (
	"github.com/authzed/spicedb/pkg/migrate"

	"github.com/jackc/pgx/v4"
)

var (
	noNonAtomicMigration migrate.MigrationFunc[*pgx.Conn]
	noAtomicMigration    migrate.TxMigrationFunc[pgx.Tx]
)

// CRDBMigrations implements a migration manager for the CRDBDriver.
var CRDBMigrations = migrate.NewManager[*CRDBDriver, *pgx.Conn, pgx.Tx]()
