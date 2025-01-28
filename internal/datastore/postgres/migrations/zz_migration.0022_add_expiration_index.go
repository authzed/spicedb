package migrations

import (
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
)

func init() {
	registerIndexMigration(
		schema.IndexExpiringRelationships,
		"add-expiration-cleanup-index",
		"add-expiration-support",
	)
}
