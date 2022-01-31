package migrations

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

const createNamespaceConfig = `CREATE TABLE namespace_config (
	namespace VARCHAR(127) NOT NULL,
	serialized_config BLOB NOT NULL,
	created_transaction BIGINT NOT NULL,
	deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
	CONSTRAINT pk_namespace_config PRIMARY KEY (namespace, created_transaction)
);`

// 127 max varchar length due to  unique constraints
const createRelationTuple = `CREATE TABLE relation_tuple (
		id BIGINT NOT NULL AUTO_INCREMENT,
		namespace VARCHAR(127) NOT NULL,
		object_id VARCHAR(127) NOT NULL,
		relation VARCHAR(127) NOT NULL,
		userset_namespace VARCHAR(127) NOT NULL,
		userset_object_id VARCHAR(127) NOT NULL,
		userset_relation VARCHAR(127) NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		PRIMARY KEY (id),
		CONSTRAINT uq_relation_tuple_namespace UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_transaction, deleted_transaction),
		CONSTRAINT uq_relation_tuple_living UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_transaction),
		CONSTRAINT uq_namespace_living UNIQUE (namespace, deleted_transaction)
	);`

const createRelationTupleTransaction = `CREATE TABLE relation_tuple_transaction (
		id BIGINT NOT NULL AUTO_INCREMENT,
		timestamp DATETIME DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);`

func init() {
	if err := Manager.Register("namespace-tables", "initial", func(mysql *MysqlDriver) error {
		tx, err := mysql.db.Beginx()
		if err != nil {
			return err
		}
		defer func() {
			log.Err(tx.Rollback())
		}()

		statements := []string{
			createNamespaceConfig,
			createRelationTuple,
			createRelationTupleTransaction,
		}

		for _, stmt := range statements {
			_, err := tx.Exec(stmt)
			if err != nil {
				return fmt.Errorf("failed to run statement: %w", err)
			}
		}

		return tx.Commit()
	}); err != nil {
		panic("failed to register migration  " + err.Error())
	}
}
