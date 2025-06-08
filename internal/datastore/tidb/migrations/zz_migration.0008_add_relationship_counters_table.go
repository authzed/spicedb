package migrations

import "fmt"

func addRelationshipCountersTable(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(128) NOT NULL,
		serialized_filter BLOB NOT NULL,
		current_count BIGINT NOT NULL DEFAULT 0,
		count_updated_at_revision BIGINT NOT NULL DEFAULT 0,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		CONSTRAINT uq_relationship_counter_living UNIQUE (name, deleted_transaction)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.RelationshipCounters(),
	)
}

func init() {
	mustRegisterMigration("add_relationship_counters_table", "watch_api_relation_tuple_index", noNonatomicMigration,
		newStatementBatch(
			addRelationshipCountersTable,
		).execute,
	)
}
