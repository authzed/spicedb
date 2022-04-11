package migrations

import "fmt"

const (
	tableNamespaceDefault   = "namespace_config"
	tableTransactionDefault = "relation_tuple_transaction"
	tableTupleDefault       = "relation_tuple"
	tableMigrationVersion   = "mysql_migration_version"
)

type tables struct {
	tableMigrationVersion string
	tableTransaction      string
	tableTuple            string
	tableNamespace        string
}

func newTables(prefix string) *tables {
	return &tables{
		tableMigrationVersion: fmt.Sprintf("%s%s", prefix, tableMigrationVersion),
		tableTransaction:      fmt.Sprintf("%s%s", prefix, tableTransactionDefault),
		tableTuple:            fmt.Sprintf("%s%s", prefix, tableTupleDefault),
		tableNamespace:        fmt.Sprintf("%s%s", prefix, tableNamespaceDefault),
	}
}

func (tn *tables) migrationVersion() string {
	return tn.tableMigrationVersion
}

func (tn *tables) RelationTupleTransaction() string {
	return tn.tableTransaction
}

func (tn *tables) RelationTuple() string {
	return tn.tableTuple
}

func (tn *tables) Namespace() string {
	return tn.tableNamespace
}
