package migrations

import "github.com/authzed/spicedb/pkg/migrate"

// DatabaseMigrations implements a migration manager for the Postgres Driver.
var DatabaseMigrations = migrate.NewManager[*AlembicPostgresDriver]()
