package migrations

import (
	"context"
	"errors"
	"fmt"
)

type driverExecutor func(mysqlDriver *MysqlDriver) string

type executor struct {
	statements []driverExecutor
}

func newExecutor(statements ...driverExecutor) executor {
	return executor{
		statements: statements,
	}
}

func (e executor) migrate(driver *MysqlDriver) error {
	if len(e.statements) == 0 {
		return errors.New("executor.migrate: No statements to migrate")
	}

	tx, err := driver.db.Begin()
	if err != nil {
		return err
	}
	defer LogOnError(context.Background(), tx.Rollback)

	for _, stmt := range e.statements {
		_, err := tx.Exec(stmt(driver))
		if err != nil {
			return fmt.Errorf("executor.migrate: failed to run statement: %w", err)
		}
	}

	return tx.Commit()
}
