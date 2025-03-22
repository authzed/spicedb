package migrations

import "github.com/authzed/spicedb/internal/datastore/postgres/schema"

func init() {
	registerIndexMigration(schema.IndexSortedRelationTupleTransaction,
		"add-index-for-transaction-gc",
		"add-expiration-cleanup-index",
	)
}
