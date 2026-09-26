package common

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestPendingTransactionLifecycle(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "failed commit"}[fail], func(t *testing.T) {
			commits, rollbacks := 0, 0
			sentinel := errors.New("commit failed")
			pending := NewPendingTransaction(func(context.Context) (datastore.Revision, error) {
				commits++
				if fail {
					return revisions.NewForTransactionID(42), sentinel
				}
				return revisions.NewForTransactionID(42), nil
			}, func(context.Context) error { rollbacks++; return nil })
			rev, err := pending.Commit(t.Context())
			if fail {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, datastore.NoRevision, rev)
			} else {
				require.NoError(t, err)
				require.Equal(t, revisions.NewForTransactionID(42), rev)
			}
			rev, err = pending.Commit(t.Context())
			require.ErrorIs(t, err, ErrTransactionClosed)
			require.Equal(t, datastore.NoRevision, rev)
			require.Equal(t, 1, commits)
			err = pending.Rollback(t.Context())
			if fail {
				require.NoError(t, err)
				require.Equal(t, 1, rollbacks)
			} else {
				require.ErrorIs(t, err, ErrTransactionClosed)
				require.Zero(t, rollbacks)
			}
			require.ErrorIs(t, pending.Rollback(t.Context()), ErrTransactionClosed)
		})
	}
}
