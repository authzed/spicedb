package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
)

// MigrateIfNeeded applies pending migrations to head through an acquired
// connection from the caller's pool. It does not close or reconfigure the pool.
func MigrateIfNeeded(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("postgres: nil pool")
	}
	ctx = withDefaultMigrationBatchSize(ctx)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return migrations.MigrateToHead(ctx, conn.Conn())
}

func withDefaultMigrationBatchSize(ctx context.Context) context.Context {
	if ctx.Value(migrate.BackfillBatchSize) != nil {
		return ctx
	}

	return context.WithValue(ctx, migrate.BackfillBatchSize, uint64(1000))
}
