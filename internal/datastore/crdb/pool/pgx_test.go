package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// recordingTx records the context passed to Rollback. All other pgx.Tx
// methods panic via the embedded nil interface; beginFuncExec only calls
// Rollback and Commit.
type recordingTx struct {
	pgx.Tx
	rollbackCtxs []context.Context
}

func (r *recordingTx) Rollback(ctx context.Context) error {
	r.rollbackCtxs = append(r.rollbackCtxs, ctx)
	return nil
}

func (r *recordingTx) Commit(_ context.Context) error {
	return nil
}

func TestBeginFuncExecRollsBackWithSeveredContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	tx := &recordingTx{}
	err := beginFuncExec(ctx, tx, func(pgx.Tx) error {
		return errors.New("txn failed")
	})
	require.Error(t, err)

	require.NotEmpty(t, tx.rollbackCtxs)
	for _, rctx := range tx.rollbackCtxs {
		require.NoError(t, rctx.Err(), "rollback must run with a severed context so it reaches the server")
	}
}
