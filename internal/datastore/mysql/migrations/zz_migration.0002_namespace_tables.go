package migrations

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

const createNamespaceConfig = `CREATE TABLE namespace_config (
	namespace VARCHAR(255) NOT NULL,
	serialized_config BLOB NOT NULL,
	created_transaction BIGINT NOT NULL,
	deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
	CONSTRAINT pk_namespace_config PRIMARY KEY (namespace, created_transaction)
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
