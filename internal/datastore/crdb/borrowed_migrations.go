package crdb

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
)

// MigrateIfNeeded applies pending migrations to head through an acquired
// connection from the caller's pool. It does not close or reconfigure the pool.
func MigrateIfNeeded(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("crdb: nil pool")
	}
	if ctx.Value(migrate.BackfillBatchSize) == nil {
		ctx = context.WithValue(ctx, migrate.BackfillBatchSize, uint64(1000))
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return migrations.MigrateToHead(ctx, conn.Conn())
}
