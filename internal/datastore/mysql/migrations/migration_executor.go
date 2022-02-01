package migrations

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

type migrationExecutor struct {
	statements []string
}

func newMigrationExecutor(statements ...string) migrationExecutor {
	return migrationExecutor{
		statements: statements,
	}
}

func (me migrationExecutor) migrate(mysql *MysqlDriver) error {
	if len(me.statements) == 0 {
		return errors.New("migrationExecutor.migrate: No statements to migrate")
	}

	tx, err := mysql.db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		log.Err(tx.Rollback())
	}()

	for _, stmt := range me.statements {
		_, err := tx.Exec(stmt)
		if err != nil {
			return fmt.Errorf("migrationExecutor.migrate: failed to run statement: %w", err)
		}
	}

	return tx.Commit()
}
