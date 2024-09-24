package migrations

const (
	tableNamespaceDefault     = "namespace_config"
	tableTransactionDefault   = "relation_tuple_transaction"
	tableTupleDefault         = "relation_tuple"
	tableMigrationVersion     = "mysql_migration_version"
	tableMetadataDefault      = "mysql_metadata"
	tableCaveatDefault        = "caveat"
	tableRelationshipCounters = "relationship_counters"
)

type tables struct {
	tableMigrationVersion     string
	tableTransaction          string
	tableTuple                string
	tableNamespace            string
	tableMetadata             string
	tableCaveat               string
	tableRelationshipCounters string
}

func newTables(prefix string) *tables {
	return &tables{
		tableMigrationVersion:     prefix + tableMigrationVersion,
		tableTransaction:          prefix + tableTransactionDefault,
		tableTuple:                prefix + tableTupleDefault,
		tableNamespace:            prefix + tableNamespaceDefault,
		tableMetadata:             prefix + tableMetadataDefault,
		tableCaveat:               prefix + tableCaveatDefault,
		tableRelationshipCounters: prefix + tableRelationshipCounters,
	}
}

func (tn *tables) migrationVersion() string {
	return tn.tableMigrationVersion
}

// RelationTupleTransaction returns the prefixed transaction table name.
func (tn *tables) RelationTupleTransaction() string {
	return tn.tableTransaction
}

// RelationTuple returns the prefixed relationship tuple table name.
func (tn *tables) RelationTuple() string {
	return tn.tableTuple
}

// Namespace returns the prefixed namespace table name.
func (tn *tables) Namespace() string {
	return tn.tableNamespace
}

func (tn *tables) Metadata() string {
	return tn.tableMetadata
}

func (tn *tables) Caveat() string {
	return tn.tableCaveat
}

func (tn *tables) RelationshipCounters() string {
	return tn.tableRelationshipCounters
}
