package common

import (
	"context"
	"errors"
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
)

// ErrTransactionIsolation rejects externally supplied transactions with unsafe isolation.
var ErrTransactionIsolation = errors.New("transaction must use SERIALIZABLE isolation")

// ErrTransactionReadOnly rejects transactions that cannot stage writes.
var ErrTransactionReadOnly = errors.New("transaction must be active and read/write")

// ErrTransactionIdentity rejects transactions for another datastore.
var ErrTransactionIdentity = errors.New("transaction belongs to another datastore")

// ErrTransactionAlreadyBound rejects a second relationship session in the same transaction.
var ErrTransactionAlreadyBound = errors.New("transaction already bound")

// ErrTransactionClosed marks an already completed pending transaction.
var ErrTransactionClosed = errors.New("transaction already completed")

// PendingTransaction finalizes and commits an adopted transaction without retries.
type PendingTransaction interface {
	Commit(context.Context) (datastore.Revision, error)
	Rollback(context.Context) error
}

// NewPendingTransaction captures finalization without exposing a provisional revision.
func NewPendingTransaction(commit func(context.Context) (datastore.Revision, error), rollback func(context.Context) error) PendingTransaction {
	return &pendingTransaction{commit: commit, rollback: rollback}
}

type pendingTransaction struct {
	mu       sync.Mutex
	done     bool // GUARDED_BY(mu)
	finished bool // GUARDED_BY(mu)
	commit   func(context.Context) (datastore.Revision, error)
	rollback func(context.Context) error
}

func (t *pendingTransaction) Commit(ctx context.Context) (datastore.Revision, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return datastore.NoRevision, ErrTransactionClosed
	}
	t.done = true
	rev, err := t.commit(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}
	t.finished = true
	return rev, nil
}

func (t *pendingTransaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return ErrTransactionClosed
	}
	t.done = true
	t.finished = true
	return t.rollback(ctx)
}
