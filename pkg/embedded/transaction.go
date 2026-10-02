package embedded

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ErrTransactionIsolation indicates that a supplied transaction is not SERIALIZABLE.
var ErrTransactionIsolation = common.ErrTransactionIsolation

// ErrTransactionReadOnly indicates that a transaction cannot stage writes.
var ErrTransactionReadOnly = common.ErrTransactionReadOnly

// ErrTransactionIdentity indicates a transaction targets another datastore.
var ErrTransactionIdentity = common.ErrTransactionIdentity

// ErrTransactionAlreadyBound indicates an existing relationship session in this transaction.
var ErrTransactionAlreadyBound = common.ErrTransactionAlreadyBound

// ErrTransactionClosed indicates a completed pending transaction.
var ErrTransactionClosed = common.ErrTransactionClosed

// ErrSessionClosed indicates use outside the relationship callback.
var ErrSessionClosed = errors.New("embedded: relationship transaction session is closed")

// PendingTransaction commits or rolls back staged application and relationship changes.
// Commit must be called instead of committing the original transaction directly.
type PendingTransaction struct{ pending common.PendingTransaction }

// Commit returns a revision only after the original transaction commits successfully.
func (t *PendingTransaction) Commit(ctx context.Context) (CommitResult, error) {
	rev, err := t.pending.Commit(ctx)
	if err != nil {
		return CommitResult{datastore.NoRevision}, err
	}
	return CommitResult{rev}, nil
}

// Rollback abandons the original transaction, including application writes.
func (t *PendingTransaction) Rollback(ctx context.Context) error { return t.pending.Rollback(ctx) }

// RelationshipTransaction exposes transaction-local reads and validated relationship writes.
// It is only usable synchronously within its callback and deliberately has no permission checks.
type RelationshipTransaction struct {
	p      *Permissions
	tx     datalayer.ReadWriteTransaction
	closed atomic.Bool
}

func (t *RelationshipTransaction) check() error {
	if t.closed.Load() {
		return ErrSessionClosed
	}
	return nil
}

// ReadSchema returns the schema visible inside this transaction.
func (t *RelationshipTransaction) ReadSchema(ctx context.Context) (ReadSchemaResult, error) {
	if err := t.check(); err != nil {
		return ReadSchemaResult{}, err
	}
	return readSchema(ctx, t.tx)
}

// WriteRelationships stages updates; Revision is NoRevision until the outer transaction commits.
func (t *RelationshipTransaction) WriteRelationships(ctx context.Context, updates []tuple.RelationshipUpdate) (WriteRelationshipsResult, error) {
	result := WriteRelationshipsResult{datastore.NoRevision}
	if err := t.check(); err != nil {
		return result, err
	}
	return result, t.p.writeRelationships(ctx, t.tx, updates)
}

// ReadRelationships yields transaction-local relationships. Iteration must finish within the callback.
func (t *RelationshipTransaction) ReadRelationships(ctx context.Context, f datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (ReadRelationshipsResult, error) {
	if err := t.check(); err != nil {
		return ReadRelationshipsResult{}, err
	}
	result, err := readRelationships(ctx, t.tx, f, opts...)
	if err != nil {
		return result, err
	}
	it := result.Relationships
	result.Relationships = func(yield func(tuple.Relationship, error) bool) {
		if err := t.check(); err != nil {
			yield(tuple.Relationship{}, err)
			return
		}
		for rel, err := range it {
			if closed := t.check(); closed != nil {
				yield(tuple.Relationship{}, closed)
				return
			}
			if !yield(rel, err) {
				return
			}
		}
	}
	return result, nil
}

func (p *Permissions) transactionCallback(fn func(context.Context, *RelationshipTransaction) error) datastore.TxUserFunc {
	return func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		session := &RelationshipTransaction{p: p, tx: datalayer.NewReadWriteTransaction(rwt, p.config.SchemaMode)}
		defer session.closed.Store(true)
		return fn(ctx, session)
	}
}
