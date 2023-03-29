package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	createNamespaceConfig = `CREATE TABLE namespace_config (
    namespace VARCHAR PRIMARY KEY,
    serialized_config BYTEA NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL
);`

	createRelationTuple = `CREATE TABLE relation_tuple (
    namespace VARCHAR NOT NULL,
    object_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    userset_namespace VARCHAR NOT NULL,
    userset_object_id VARCHAR NOT NULL,
    userset_relation VARCHAR NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
    CONSTRAINT pk_relation_tuple PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)
);`

	createSchemaVersion = `CREATE TABLE schema_version (
	version_num VARCHAR NOT NULL
);`

	insertEmptyVersion = `INSERT INTO schema_version (version_num) VALUES ('');`

	createReverseQueryIndex = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`
	createReverseCheckIndex = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (userset_namespace, userset_relation, namespace, relation)`
)

func init() {
	if err := CRDBMigrations.Register("initial", "", noNonAtomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		statements := []string{
			createNamespaceConfig,
			createRelationTuple,
			createSchemaVersion,
			insertEmptyVersion,
			createReverseQueryIndex,
			createReverseCheckIndex,
		}
		for _, stmt := range statements {
			_, err := tx.Exec(ctx, stmt)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
