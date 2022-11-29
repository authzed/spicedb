package migrations

import "fmt"

// we need to get some index headroom back in order to add caveat_name to the covering index
// 128 is already enforced at the application level
func shrinkCaveatNameLengthRelationTupleTable(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s MODIFY caveat_name VARCHAR(128);`, t.RelationTuple())
}

// 128 is already enforced at the application level
func shrinkCaveatNameLengthCaveatTable(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s MODIFY name VARCHAR(128);`, t.Caveat())
}

// We cannot do a covering index including the JSON caveat_context column. Unfortunately MySQL couples covering
// index with the index definition itself, compared to other databases.
// - JSON column cannot be indexed
// - generated column over JSON column does not fit max index length 3072 bytes
// - functional indexes lead to the same index length
func createCaveatCoveringIndex(t *tables) string {
	return fmt.Sprintf(`CREATE INDEX ix_relation_tuple_caveat_covering 
		ON %s (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, caveat_name);`,
		t.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("add_caveat_covering_index", "add_caveat", noNonatomicMigration,
		newStatementBatch(
			shrinkCaveatNameLengthCaveatTable,
			shrinkCaveatNameLengthRelationTupleTable,
			createCaveatCoveringIndex).
			execute,
	)
}
