package embedded

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
)

// pgxTransaction preserves pgx's interface, including Conn and savepoint behavior.
// It intentionally does not add revision-returning semantics to pgx.Commit.
type pgxTransaction struct {
	pgx.Tx
	mu     sync.Mutex
	closed bool // GUARDED_BY(mu)
}

var _ pgx.Tx = (*pgxTransaction)(nil)

func wrapPGXTransaction(tx pgx.Tx) pgx.Tx { return &pgxTransaction{Tx: tx} }
func (t *pgxTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return pgx.ErrTxClosed
	}
	t.closed = true
	return t.Tx.Commit(ctx)
}

func (t *pgxTransaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return pgx.ErrTxClosed
	}
	t.closed = true
	return t.Tx.Rollback(ctx)
}
