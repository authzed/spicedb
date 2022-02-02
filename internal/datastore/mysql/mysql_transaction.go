package mysql

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/jmoiron/sqlx"
)

type mysqlTransaction struct {
	tx *sqlx.Tx
}

func (mysqlTx *mysqlTransaction) Query(ctx context.Context, sql string, args ...interface{}) (common.DbRows, error) {
	rows, err := mysqlTx.tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &mysqlDbRows{dbRows: rows}, nil
}

func (mysqlTx *mysqlTransaction) Rollback(ctx context.Context) error {
	return mysqlTx.tx.Rollback()
}
