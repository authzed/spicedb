package migrations

import "fmt"

func addWatchAPIIndex(t *tables) string {
	return fmt.Sprintf(`CREATE INDEX ix_relation_tuple_watch
    ON %s (created_transaction, deleted_transaction DESC);`, t.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("watch_api_relation_tuple_index", "longblob_definitions", noNonatomicMigration,
		newStatementBatch(
			addWatchAPIIndex,
		).execute,
	)
}
