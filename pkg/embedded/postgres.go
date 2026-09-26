package embedded

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/pkg/datastore"
)

// PostgresConfig configures an embedded datastore borrowing a Postgres pool.
type PostgresConfig struct {
	Permissions Config
	GCWindow    time.Duration
	GCInterval  time.Duration
}

// MigrateIfNeeded applies any pending migrations through pool before the
// embedded datastore is constructed. The pool remains caller-owned.
func (PostgresConfig) MigrateIfNeeded(ctx context.Context, pool *pgxpool.Pool) error {
	return postgres.MigrateIfNeeded(ctx, pool)
}

// PostgresPermissions provides validated operations over an application-owned pool.
type PostgresPermissions struct {
	*Permissions
	pool    *pgxpool.Pool
	backend datastore.Datastore
	binder  postgres.TransactionBinder
}

// NewPostgresPermissions borrows pool; call cfg.MigrateIfNeeded first when needed.
func NewPostgresPermissions(ctx context.Context, pool *pgxpool.Pool, cfg PostgresConfig) (*PostgresPermissions, error) {
	if cfg.Permissions.Datastore != nil {
		return nil, errors.New("embedded: Datastore conflicts with supplied pool")
	}
	if cfg.GCWindow < 0 || cfg.GCInterval < 0 {
		return nil, errors.New("embedded: GC durations must not be negative")
	}
	opts := []postgres.Option{}
	if cfg.GCWindow > 0 {
		opts = append(opts, postgres.GCWindow(cfg.GCWindow))
	}
	if cfg.GCInterval > 0 {
		opts = append(opts, postgres.GCInterval(cfg.GCInterval))
	}
	ds, binder, err := postgres.NewPostgresDatastoreWithPool(ctx, pool, opts...)
	if err != nil {
		return nil, err
	}
	cfg.Permissions.Datastore = ds
	p, err := NewPermissions(cfg.Permissions)
	if err != nil {
		ds.Close()
		return nil, err
	}
	return &PostgresPermissions{p, pool, ds, binder}, nil
}

// BeginTransaction begins a read/write SERIALIZABLE transaction usable by pgx clients.
func (p *PostgresPermissions) BeginTransaction(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadWrite})
	if err != nil {
		return nil, err
	}
	return wrapPGXTransaction(tx), nil
}

// WithPostgresTransaction validates and adopts an existing transaction without committing or retrying it.
func (p *PostgresPermissions) WithPostgresTransaction(ctx context.Context, tx pgx.Tx, fn func(context.Context, *RelationshipTransaction) error) (*PendingTransaction, error) {
	if fn == nil {
		return nil, errors.New("embedded: callback is required")
	}
	pending, err := p.binder.WithTransaction(ctx, tx, postgres.ExternalTransactionOptions{LockUnifiedSchema: p.config.SchemaMode.ReadsFromNew()}, p.transactionCallback(fn))
	if err != nil {
		return nil, err
	}
	return &PendingTransaction{pending}, nil
}

// Close stops embedded resources without closing the application's pool.
func (p *PostgresPermissions) Close() error {
	return errors.Join(p.Permissions.Close(), p.backend.Close())
}
