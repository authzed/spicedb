package migrations

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

const createMysqlMigrationVersion = `CREATE TABLE mysql_migration_version (
	version_num VARCHAR(255) NOT NULL PRIMARY KEY
);`
const insertEmptyVersion = `INSERT INTO mysql_migration_version (version_num) VALUES ('');`

func init() {
	if err := DatabaseMigrations.Register("initial", "", func(mysql *MysqlDriver) error {
		tx, err := mysql.db.Beginx()
		if err != nil {
			return err
		}
		defer func() {
			log.Err(tx.Rollback())
		}()

		statements := []string{
			createMysqlMigrationVersion,
			insertEmptyVersion,
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
