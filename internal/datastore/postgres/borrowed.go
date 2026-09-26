package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

// NewPostgresDatastoreWithPool borrows a caller pool without changing its hooks or ownership.
func NewPostgresDatastoreWithPool(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (datastore.Datastore, TransactionBinder, error) {
	if pool == nil {
		return nil, nil, errors.New("postgres: nil pool")
	}
	opts = append(opts, func(o *postgresOptions) {
		o.borrowedPool = pool
		o.relaxedIsolationLevel = false
		o.enablePrometheusStats = false
	})
	ds, err := newPostgresDatastore(ctx, "", primaryInstanceID, opts...)
	if err != nil {
		return nil, nil, err
	}
	pg := ds.(*pgDatastore)
	ready, err := pg.ReadyState(ctx)
	if err != nil || !ready.IsReady {
		pg.Close()
		if err == nil {
			err = errors.New(ready.Message)
		}
		return nil, nil, err
	}
	id, err := pg.UniqueID(ctx)
	if err != nil {
		pg.Close()
		return nil, nil, err
	}
	return datastore.NewSeparatingContextDatastoreProxy(pg), &transactionBinder{pg: pg, identity: id}, nil
}

// preparedPool prepares each acquired connection without modifying caller hooks.
type preparedPool struct{ pool *pgxpool.Pool }

func (p preparedPool) Close() {}
func (p preparedPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	c, err := p.pool.Acquire(ctx)
	if err == nil {
		RegisterTypes(c.Conn().TypeMap())
	}
	return c, err
}

func (p preparedPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.BeginTx(ctx, pgx.TxOptions{})
}

func (p preparedPool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		c.Release()
		return nil, err
	}
	return &releasedTx{Tx: tx, release: c.Release}, nil
}

type releasedTx struct {
	pgx.Tx
	release func()
	once    sync.Once
}

func (t *releasedTx) Commit(ctx context.Context) error {
	defer t.once.Do(t.release)
	return t.Tx.Commit(ctx)
}

func (t *releasedTx) Rollback(ctx context.Context) error {
	defer t.once.Do(t.release)
	return t.Tx.Rollback(ctx)
}

func (p preparedPool) Exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer c.Release()
	return c.Exec(ctx, q, args...)
}

func (p preparedPool) CopyFrom(ctx context.Context, t pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	return c.CopyFrom(ctx, t, cols, src)
}

func (p preparedPool) Query(ctx context.Context, q string, args ...any) (pgx.Rows, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.Query(ctx, q, args...) //nolint:rowserrcheck // The returned Rows exposes Err() to the caller.
	if err != nil {
		c.Release()
		return nil, err
	}
	return &releasedRows{Rows: rows, release: c.Release}, nil
}

type releasedRows struct {
	pgx.Rows
	release func()
	once    sync.Once
}

func (r *releasedRows) Close() { r.Rows.Close(); r.once.Do(r.release) }
func (r *releasedRows) Next() bool {
	ok := r.Rows.Next()
	if !ok {
		r.Close()
	}
	return ok
}

func (p preparedPool) QueryRow(ctx context.Context, q string, args ...any) pgx.Row {
	rows, err := p.Query(ctx, q, args...) //nolint:rowserrcheck // The returned Rows exposes Err() to the caller.
	return preparedRow{rows, err}
}

type preparedRow struct {
	rows pgx.Rows
	err  error
}

func (r preparedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// A borrowed pool must not lose a connection to a permanent leader lock.
func (pgd *pgDatastore) startBorrowedRevisionHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(max(time.Second, time.Duration(pgd.quantizationPeriodNanos)))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := pgx.BeginTxFunc(ctx, pgd.writePool, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadWrite}, func(tx pgx.Tx) error {
				var acquired bool
				if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", revisionHeartbeatLock).Scan(&acquired); err != nil {
					return err
				}
				if !acquired {
					return nil
				}
				_, err := tx.Exec(ctx, pgd.revisionHeartbeatQuery)
				return err
			})
			if err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("borrowed pool revision heartbeat failed")
			}
		}
	}
}
