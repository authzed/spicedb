package embedded

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/pkg/datastore"
)

// CRDBConfig configures an embedded datastore borrowing a CRDB pool.
type CRDBConfig struct {
	Permissions Config
	GCWindow    time.Duration
}

// MigrateIfNeeded applies any pending migrations through pool before the
// embedded datastore is constructed. The pool remains caller-owned.
func (CRDBConfig) MigrateIfNeeded(ctx context.Context, pool *pgxpool.Pool) error {
	return crdb.MigrateIfNeeded(ctx, pool)
}

// CRDBPermissions provides validated operations over an application-owned pool.
type CRDBPermissions struct {
	*Permissions
	pool    *pgxpool.Pool
	backend datastore.Datastore
	binder  crdb.TransactionBinder
}

// NewCRDBPermissions borrows pool; call cfg.MigrateIfNeeded first when needed.
func NewCRDBPermissions(ctx context.Context, pool *pgxpool.Pool, cfg CRDBConfig) (*CRDBPermissions, error) {
	if cfg.Permissions.Datastore != nil {
		return nil, errors.New("embedded: Datastore conflicts with supplied pool")
	}
	if cfg.GCWindow < 0 {
		return nil, errors.New("embedded: GC durations must not be negative")
	}
	opts := []crdb.Option{}
	if cfg.GCWindow > 0 {
		opts = append(opts, crdb.GCWindow(cfg.GCWindow))
	}
	ds, binder, err := crdb.NewCRDBDatastoreWithPool(ctx, pool, opts...)
	if err != nil {
		return nil, err
	}
	cfg.Permissions.Datastore = ds
	p, err := NewPermissions(cfg.Permissions)
	if err != nil {
		ds.Close()
		return nil, err
	}
	return &CRDBPermissions{p, pool, ds, binder}, nil
}

// BeginTransaction begins a read/write SERIALIZABLE transaction usable by pgx clients.
func (p *CRDBPermissions) BeginTransaction(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadWrite})
	if err != nil {
		return nil, err
	}
	return wrapPGXTransaction(tx), nil
}

// WithCRDBTransaction validates and adopts an existing transaction without committing or retrying it.
func (p *CRDBPermissions) WithCRDBTransaction(ctx context.Context, tx pgx.Tx, fn func(context.Context, *RelationshipTransaction) error) (*PendingTransaction, error) {
	if fn == nil {
		return nil, errors.New("embedded: callback is required")
	}
	pending, err := p.binder.WithTransaction(ctx, tx, p.transactionCallback(fn))
	if err != nil {
		return nil, err
	}
	return &PendingTransaction{pending}, nil
}

// Close stops embedded resources without closing the application's pool.
func (p *CRDBPermissions) Close() error {
	return errors.Join(p.Permissions.Close(), p.backend.Close())
}
