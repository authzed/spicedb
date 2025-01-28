package migrations

import "github.com/authzed/spicedb/internal/datastore/postgres/schema"

func init() {
	registerIndexMigration(
		schema.IndexWatchAPI,
		"add-watch-api-index-to-relation-tuple-table",
		"add-metadata-to-transaction-table",
	)
}
