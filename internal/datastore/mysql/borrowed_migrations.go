package mysql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
)

// MigrateIfNeeded applies pending migrations to head through the caller's DB
// and table prefix. It does not close or reconfigure the DB.
func MigrateIfNeeded(ctx context.Context, db *sql.DB, tablePrefix string) error {
	if db == nil {
		return errors.New("mysql: nil DB")
	}
	return migrations.MigrateToHead(ctx, db, tablePrefix)
}
