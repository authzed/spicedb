package migrations

import "github.com/authzed/spicedb/pkg/migrate"

// CRDBMigrations implements a migration manager for the CRDBDriver.
var CRDBMigrations = migrate.NewManager()
