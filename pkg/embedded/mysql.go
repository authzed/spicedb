package embedded

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/pkg/datastore"
)

// MySQLConfig configures an embedded datastore borrowing a MySQL pool.
type MySQLConfig struct {
	Permissions Config
	GCWindow    time.Duration
	GCInterval  time.Duration
	TablePrefix string
}

// MigrateIfNeeded applies pending migrations through pool using the configured
// table prefix. The DB remains caller-owned.
func (cfg MySQLConfig) MigrateIfNeeded(ctx context.Context, pool *sql.DB) error {
	return mysql.MigrateIfNeeded(ctx, pool, cfg.TablePrefix)
}

// MySQLPermissions provides validated operations over an application-owned pool.
type MySQLPermissions struct {
	*Permissions
	pool    *sql.DB
	backend datastore.Datastore
	binder  mysql.TransactionBinder
}

// NewMySQLPermissions borrows pool; call cfg.MigrateIfNeeded first when needed.
func NewMySQLPermissions(ctx context.Context, pool *sql.DB, cfg MySQLConfig) (*MySQLPermissions, error) {
	if cfg.Permissions.Datastore != nil {
		return nil, errors.New("embedded: Datastore conflicts with supplied pool")
	}
	if cfg.GCWindow < 0 || cfg.GCInterval < 0 {
		return nil, errors.New("embedded: GC durations must not be negative")
	}
	opts := []mysql.Option{mysql.TablePrefix(cfg.TablePrefix)}
	if cfg.GCWindow > 0 {
		opts = append(opts, mysql.GCWindow(cfg.GCWindow))
	}
	if cfg.GCInterval > 0 {
		opts = append(opts, mysql.GCInterval(cfg.GCInterval))
	}
	ds, binder, err := mysql.NewMySQLDatastoreWithDB(ctx, pool, opts...)
	if err != nil {
		return nil, err
	}
	cfg.Permissions.Datastore = ds
	p, err := NewPermissions(cfg.Permissions)
	if err != nil {
		ds.Close()
		return nil, err
	}
	return &MySQLPermissions{p, pool, ds, binder}, nil
}

// BeginTransaction begins a read/write SERIALIZABLE transaction usable by database/sql clients.
func (p *MySQLPermissions) BeginTransaction(ctx context.Context) (*sql.Tx, error) {
	return p.pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false})
}

// WithMySQLTransaction validates and adopts an existing transaction without committing or retrying it.
// Application tables must use InnoDB, and the transaction must not execute statements that
// implicitly commit. The connection must use parseTime=true, and transaction instrumentation
// plus the performance_schema current-transactions consumer must be enabled and accessible.
func (p *MySQLPermissions) WithMySQLTransaction(ctx context.Context, tx *sql.Tx, fn func(context.Context, *RelationshipTransaction) error) (*PendingTransaction, error) {
	if fn == nil {
		return nil, errors.New("embedded: callback is required")
	}
	pending, err := p.binder.WithTransaction(ctx, tx, p.config.SchemaMode.ReadsFromNew(), p.transactionCallback(fn))
	if err != nil {
		return nil, err
	}
	return &PendingTransaction{pending}, nil
}

// Close stops embedded resources without closing the application's pool.
func (p *MySQLPermissions) Close() error {
	return errors.Join(p.Permissions.Close(), p.backend.Close())
}
