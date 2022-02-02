package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/jmoiron/sqlx"
)

type mysqlDbConnection struct {
	db *sqlx.DB
}

func (mysqlDbConn *mysqlDbConnection) BeginTxx(ctx context.Context, readOnly bool) (common.TransactionWrapper, error) {
	tx, err := mysqlDbConn.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: 0,
		ReadOnly:  readOnly,
	})
	if err != nil {
		return nil, fmt.Errorf(common.ErrUnableToInstantiate, err)
	}

	return &mysqlTransaction{tx: tx}, nil
}
