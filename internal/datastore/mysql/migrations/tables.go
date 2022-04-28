package migrations

import "fmt"

const (
	tableNamespaceDefault   = "namespace_config"
	tableTransactionDefault = "relation_tuple_transaction"
	tableTupleDefault       = "relation_tuple"
	tableMigrationVersion   = "mysql_migration_version"
	tableMetadataDefault    = "mysql_metadata"
)

type tables struct {
	tableMigrationVersion string
	tableTransaction      string
	tableTuple            string
	tableNamespace        string
	tableMetadata         string
}

func newTables(prefix string) *tables {
	return &tables{
		tableMigrationVersion: fmt.Sprintf("%s%s", prefix, tableMigrationVersion),
		tableTransaction:      fmt.Sprintf("%s%s", prefix, tableTransactionDefault),
		tableTuple:            fmt.Sprintf("%s%s", prefix, tableTupleDefault),
		tableNamespace:        fmt.Sprintf("%s%s", prefix, tableNamespaceDefault),
		tableMetadata:         fmt.Sprintf("%s%s", prefix, tableMetadataDefault),
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
