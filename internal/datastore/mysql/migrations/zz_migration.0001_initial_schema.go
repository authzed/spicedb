package migrations

import "fmt"

func createMigrationVersion(t *tables) string {
	// we need the additional primary key column because github.com/github/gh-ost requires a shared, not-null
	// key between the _to_ and _from_ table schemas to perform a schema migration.
	// -- https://github.com/github/gh-ost/blob/master/doc/shared-key.md
	return fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL PRIMARY KEY,
		_meta_version_ VARCHAR(255) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.migrationVersion(),
	)
}

// namespace max size: https://buf.build/authzed/api/file/main/authzed/api/v0/core.proto#L29
func createNamespaceConfig(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		namespace VARCHAR(128) NOT NULL,
		serialized_config BLOB NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		CONSTRAINT pk_namespace_config PRIMARY KEY (namespace, created_transaction),
		CONSTRAINT uq_namespace_living UNIQUE (namespace, deleted_transaction)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.Namespace(),
	)
}

// relationship max size: https://buf.build/authzed/api/file/main:authzed/api/v1/core.proto#L33
// object id max size: https://buf.build/authzed/api/file/main:authzed/api/v1/core.proto#L45
func createRelationTuple(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		namespace VARCHAR(128) NOT NULL,
		object_id VARCHAR(128) NOT NULL,
		relation VARCHAR(64) NOT NULL,
		userset_namespace VARCHAR(128) NOT NULL,
		userset_object_id VARCHAR(128) NOT NULL,
		userset_relation VARCHAR(64) NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		PRIMARY KEY (id),
		CONSTRAINT uq_relation_tuple_namespace UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_transaction, deleted_transaction),
		CONSTRAINT uq_relation_tuple_living UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_transaction),
        INDEX ix_relation_tuple_by_subject (userset_object_id, userset_namespace, userset_relation, namespace, relation),
        INDEX ix_relation_tuple_by_subject_relation (userset_namespace, userset_relation, namespace, relation),
        INDEX ix_relation_tuple_by_deleted_transaction (deleted_transaction)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.RelationTuple(),
	)
}

func createRelationTupleTransaction(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		timestamp DATETIME(6) DEFAULT NOW(6) NOT NULL,
		PRIMARY KEY (id),
        INDEX ix_relation_tuple_transaction_by_timestamp (timestamp)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		t.RelationTupleTransaction(),
	)
}

func init() {
	mustRegisterMigration("initial", "", noNonatomicMigration,
		newStatementBatch(
			createMigrationVersion,
			createNamespaceConfig,
			createRelationTuple,
			createRelationTupleTransaction,
		).execute,
	)
}
