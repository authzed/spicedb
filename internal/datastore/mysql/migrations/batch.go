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

func (e statementBatch) execute(ctx context.Context, wrapper TxWrapper) error {
	if len(e.statements) == 0 {
		return errors.New("executor.migrate: No statements to migrate")
	}

	for _, stmt := range e.statements {
		if _, err := wrapper.tx.ExecContext(ctx, stmt(wrapper.tables)); err != nil {
			return fmt.Errorf("statementBatch.execute: failed to exec statement: %w", err)
		}
	}

	return nil
}
