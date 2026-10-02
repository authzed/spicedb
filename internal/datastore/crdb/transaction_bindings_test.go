package crdb

import (
	"context"
	"runtime"
	"testing"
	"weak"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
)

type bindingTestTx struct {
	pgx.Tx
	conn   *pgx.Conn
	closed bool
}

func (t *bindingTestTx) Conn() *pgx.Conn { return t.conn }
func (t *bindingTestTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	if t.closed {
		return pgconn.CommandTag{}, pgx.ErrTxClosed
	}
	return pgconn.CommandTag{}, nil
}

func TestTransactionBindings(t *testing.T) {
	b := &transactionBinder{}
	raw := &bindingTestTx{conn: &pgx.Conn{}}
	token, err := b.reserveTransaction(t.Context(), raw)
	require.NoError(t, err)
	// A different wrapper around the same active connection cannot adopt twice.
	_, err = (&transactionBinder{}).reserveTransaction(t.Context(), struct{ pgx.Tx }{raw})
	require.ErrorIs(t, err, common.ErrTransactionAlreadyBound)
	// A retained pending handle for a rolled-back transaction must not prevent
	// a later transaction from borrowing the same physical connection.
	raw.closed = true
	next, err := b.reserveTransaction(t.Context(), &bindingTestTx{conn: raw.conn})
	require.NoError(t, err)
	b.forget(token)
	_, err = b.reserveTransaction(t.Context(), next.tx)
	require.ErrorIs(t, err, common.ErrTransactionAlreadyBound)
	b.forget(next)
}

func TestTransactionBindingsDoNotRetainAbandonedTransactions(t *testing.T) {
	b := &transactionBinder{}
	conn := &pgx.Conn{}
	abandoned := func() weak.Pointer[bindingTestTx] {
		raw := &bindingTestTx{conn: conn}
		token, err := b.reserveTransaction(t.Context(), raw)
		require.NoError(t, err)
		runtime.KeepAlive(token)
		return weak.Make(raw)
	}()
	runtime.GC()
	require.Nil(t, abandoned.Value())
	token, err := b.reserveTransaction(t.Context(), &bindingTestTx{conn: conn})
	require.NoError(t, err)
	b.forget(token)
}
