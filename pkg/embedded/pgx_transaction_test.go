package embedded

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type countingTx struct {
	pgx.Tx
	commits, rollbacks int
}

func (t *countingTx) Commit(context.Context) error   { t.commits++; return nil }
func (t *countingTx) Rollback(context.Context) error { t.rollbacks++; return nil }
func TestPGXTransactionCompletion(t *testing.T) {
	raw := &countingTx{}
	tx := wrapPGXTransaction(raw)
	require.NoError(t, tx.Commit(t.Context()))
	require.ErrorIs(t, tx.Commit(t.Context()), pgx.ErrTxClosed)
	require.ErrorIs(t, tx.Rollback(t.Context()), pgx.ErrTxClosed)
	require.Equal(t, 1, raw.commits)
	require.Zero(t, raw.rollbacks)
}
