package migrations

import (
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
)

func init() {
	registerIndexMigration(
		schema.IndexRelationshipByResourceIDPrefix,
		"add-resource-id-prefix-index",
		"populate-schema-tables",
	)
}
