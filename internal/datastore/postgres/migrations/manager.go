package migrations

import "github.com/authzed/spicedb/pkg/migrate"

// Manager implements a migration manager for the Postgres Driver.
var Manager = migrate.NewManager()
