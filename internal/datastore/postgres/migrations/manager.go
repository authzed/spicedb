package migrations

import (
	"github.com/authzed/spicedb/pkg/migrate"

	"github.com/jackc/pgx/v4"
)

// DatabaseMigrations implements a migration manager for the Postgres Driver.
var DatabaseMigrations = migrate.NewManager[*AlembicPostgresDriver, pgx.Tx]()
