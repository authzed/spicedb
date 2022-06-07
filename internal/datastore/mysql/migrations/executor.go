package migrations

import (
	"context"
	"errors"
	"fmt"
)

type driverExecutor func(mysqlDriver *MySQLDriver) string

type executor struct {
	statements []driverExecutor
}

func newExecutor(statements ...driverExecutor) executor {
	return executor{
		statements: statements,
	}
}

func (e executor) migrate(ctx context.Context, driver *MySQLDriver) error {
	if len(e.statements) == 0 {
		return errors.New("executor.migrate: No statements to migrate")
	}

	tx, err := driver.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer LogOnError(ctx, tx.Rollback)

	for _, stmt := range e.statements {
		_, err := tx.ExecContext(ctx, stmt(driver))
		if err != nil {
			return fmt.Errorf("executor.migrate: failed to run statement: %w", err)
		}
	}

	return tx.Commit()
}
