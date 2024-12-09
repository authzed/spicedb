package migrations

import (
	"context"

	"cirello.io/pglock"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/postgres/common"
)

func init() {
	if err := DatabaseMigrations.Register("add-gc-lock-table", "add-expiration-support",
		func(ctx context.Context, conn *pgx.Conn) error {
			return common.RunWithLocksClient(conn, func(client *pglock.Client) error {
				return client.TryCreateTable()
			})
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
