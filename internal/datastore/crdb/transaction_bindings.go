package crdb

import (
	"context"
	"errors"
	"sync"
	"weak"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
)

// A pgx connection can have only one top-level transaction at a time. Sharing
// this non-owning registry also rejects alternate wrappers and facades. Pending
// handles retain their binding; abandoning a handle retains neither it nor its tx.
var adoptedTransactions = struct {
	sync.Mutex
	entries map[weak.Pointer[pgx.Conn]]weak.Pointer[transactionBinding] // GUARDED_BY(Mutex)
}{entries: make(map[weak.Pointer[pgx.Conn]]weak.Pointer[transactionBinding])}

type transactionBinding struct{ tx pgx.Tx }

func (b *transactionBinder) reserveTransaction(ctx context.Context, tx pgx.Tx) (*transactionBinding, error) {
	key := weak.Make(tx.Conn())
	for {
		adoptedTransactions.Lock()
		for k, v := range adoptedTransactions.entries {
			if k.Value() == nil || v.Value() == nil {
				delete(adoptedTransactions.entries, k)
			}
		}
		previous := adoptedTransactions.entries[key].Value()
		if previous == nil {
			binding := &transactionBinding{tx: tx}
			adoptedTransactions.entries[key] = weak.Make(binding)
			adoptedTransactions.Unlock()
			return binding, nil
		}
		adoptedTransactions.Unlock()
		// Raw rollback is supported for error cleanup. Its old pending handle can
		// remain live while the physical connection is reused by a new transaction.
		// pgx rejects an empty command on a closed transaction without sending SQL;
		// an active one executes a harmless empty command and remains bound.
		_, err := previous.tx.Exec(ctx, "")
		if !errors.Is(err, pgx.ErrTxClosed) {
			return nil, errors.Join(common.ErrTransactionAlreadyBound, err)
		}
		b.forget(previous)
	}
}

func (*transactionBinder) forget(binding *transactionBinding) {
	key := weak.Make(binding.tx.Conn())
	adoptedTransactions.Lock()
	defer adoptedTransactions.Unlock()
	if adoptedTransactions.entries[key].Value() == binding {
		delete(adoptedTransactions.entries, key)
	}
}
