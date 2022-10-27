package migrations

import "fmt"

// primary key size limit in InnoDB is 3KB.
// 3072 / 4 bytes per character = 768 - 2 bigints (4 bytes) = 764 bytes.
// we choose 700 to leave headroom for potential future new columns in the PK
func createCaveatTable(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		name VARCHAR(700) NOT NULL,
		definition BLOB NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		CONSTRAINT pk_caveat PRIMARY KEY (name, deleted_transaction),
		CONSTRAINT uq_caveat UNIQUE (name, created_transaction, deleted_transaction)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.Caveat(),
	)
}

// given column size subtracts from the max allowed row size, we define a conservative value of 128 bytes
// for caveat name. This gives larger headroom for JSON column size.
// See https://dev.mysql.com/doc/refman/5.7/en/char.html
func addCaveatToRelationTuplesTable(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s
			ADD COLUMN caveat_name VARCHAR(700),
			ADD COLUMN caveat_context JSON;`,
		t.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("add_caveat", "add_ns_config_id", noNonatomicMigration,
		newStatementBatch(
			createCaveatTable,
			addCaveatToRelationTuplesTable,
		).execute,
	)
}
