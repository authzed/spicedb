package migrations

import "github.com/authzed/spicedb/internal/datastore/postgres/schema"

func init() {
	registerIndexMigration(schema.IndexBackwardRelationships,
		"add-new-reverse-relationship-index",
		"add-new-forward-relationship-index",
	)
}
