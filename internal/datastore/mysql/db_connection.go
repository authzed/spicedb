package mysql

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/jmoiron/sqlx"
)

type mysqlDbConnection struct {
	db *sqlx.DB
}

func (mysqlDbConn *mysqlDbConnection) BeginTxx(ctx context.Context, readOnly bool) (*mysqlTransaction, error) {
	tx, err := mysqlDbConn.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf(common.ErrUnableToInstantiate, err)
	}
	defer tx.Rollback()

	return &mysqlTransaction{tx: tx}, nil
}
