package migrations

import (
	"context"
	"errors"
	"fmt"
)

type templatedStatement func(tx *tables) string

type statementBatch struct {
	statements []templatedStatement
}

func newStatementBatch(statements ...templatedStatement) statementBatch {
	return statementBatch{
		statements: statements,
	}
}

func (e statementBatch) execute(ctx context.Context, driver Wrapper, version, replaced string) error {
	if len(e.statements) == 0 {
		return errors.New("executor.migrate: No statements to migrate")
	}

	tx, err := driver.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, stmt := range e.statements {
		_, err := tx.ExecContext(ctx, stmt(driver.Tables))
		if err != nil {
			return fmt.Errorf("statementBatch.execute: failed to exec statement: %w", err)
		}
	}
	err = writeVersion(ctx, tx, driver.Tables.migrationVersion(), version, replaced)
	if err != nil {
		return err
	}
	return tx.Commit()
}
