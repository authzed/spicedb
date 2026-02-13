package migrations

import "fmt"

func createSchemaTable(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		name VARCHAR(700) NOT NULL,
		chunk_index INT NOT NULL,
		chunk_data LONGBLOB NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		CONSTRAINT pk_schema PRIMARY KEY (name, chunk_index, created_transaction)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.Schema(),
	)
}

func createSchemaRevisionTable(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		name VARCHAR(700) NOT NULL DEFAULT 'current',
		hash BLOB NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		CONSTRAINT pk_schema_revision PRIMARY KEY (name, created_transaction)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.SchemaRevision(),
	)
}

func init() {
	mustRegisterMigration("add_schema_tables", "add_expiration_to_relation_tuple", noNonatomicMigration,
		newStatementBatch(
			createSchemaTable,
			createSchemaRevisionTable,
		).execute,
	)
}
