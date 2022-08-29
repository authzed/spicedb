package migrations

import "fmt"

func addCaveatColumn(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s ADD COLUMN caveat BLOB;`, t.RelationTuple())
}

func init() {
	mustRegisterMigration("add_caveat", "add_ns_config_id", noNonatomicMigration,
		newStatementBatch(
			addCaveatColumn,
		).execute,
	)
}
