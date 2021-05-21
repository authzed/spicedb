package migrations

import "context"

const createNamespaceConfig = `CREATE TABLE namespace_config (
    namespace VARCHAR PRIMARY KEY,
    serialized_config BYTEA NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL
);`

const createRelationTuple = `CREATE TABLE relation_tuple (
    namespace VARCHAR NOT NULL,
    object_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    userset_namespace VARCHAR NOT NULL,
    userset_object_id VARCHAR NOT NULL,
    userset_relation VARCHAR NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
    CONSTRAINT pk_relation_tuple PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)
);`

const createSchemaVersion = `CREATE TABLE schema_version (
	version_num VARCHAR NOT NULL
);`

const insertEmptyVersion = `INSERT INTO schema_version (version_num) VALUES ('');`

const enableRangefeeds = `SET CLUSTER SETTING kv.rangefeed.enabled = true;`

func init() {
	CRDBMigrations.Register("initial", "", func(apd *CRDBDriver) error {
		ctx := context.Background()

		_, err := apd.db.Exec(ctx, enableRangefeeds)
		if err != nil {
			return err
		}

		tx, err := apd.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		statements := []string{
			createNamespaceConfig,
			createRelationTuple,
			createSchemaVersion,
			insertEmptyVersion,
		}
		for _, stmt := range statements {
			_, err := tx.Exec(ctx, stmt)
			if err != nil {
				return err
			}
		}

		return tx.Commit(ctx)
	})
}
