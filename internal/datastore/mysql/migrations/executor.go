package migrations

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

type executor struct {
	statements []string
}

func newExecutor(statements ...string) executor {
	return executor{
		statements: statements,
	}
}

func (me executor) migrate(mysql *MysqlDriver) error {
	if len(me.statements) == 0 {
		return errors.New("executor.migrate: No statements to migrate")
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
			return fmt.Errorf("executor.migrate: failed to run statement: %w", err)
		}
	}

	return tx.Commit()
}
