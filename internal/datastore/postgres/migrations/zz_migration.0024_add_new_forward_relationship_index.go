package migrations

import "github.com/authzed/spicedb/internal/datastore/postgres/schema"

func init() {
	registerIndexMigration(schema.IndexForwardRelationships,
		"add-new-forward-relationship-index",
		"add-index-for-transaction-gc",
	)
}
