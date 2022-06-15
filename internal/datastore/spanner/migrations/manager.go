package migrations

import (
	"github.com/authzed/spicedb/pkg/migrate"
)

// SpannerMigrations implements a migration manager for the Spanner datastore.
var SpannerMigrations = migrate.NewManager[*SpannerMigrationDriver, Wrapper]()
