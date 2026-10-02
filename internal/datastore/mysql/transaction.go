package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"weak"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TransactionBinder adopts an existing MySQL transaction without retrying it.
type TransactionBinder interface {
	WithTransaction(context.Context, *sql.Tx, bool, datastore.TxUserFunc) (common.PendingTransaction, error)
}
type transactionBinder struct {
	ds       *mysqlDatastore
	identity string
}

// Binding is shared across facades and never owns the caller's transaction.
var adoptedTransactions = struct {
	sync.Mutex
	bound map[weak.Pointer[sql.Tx]]struct{} // GUARDED_BY(Mutex)
}{bound: make(map[weak.Pointer[sql.Tx]]struct{})}

// NewMySQLDatastoreWithDB borrows a migrated DB. It never closes or reconfigures the DB.
func NewMySQLDatastoreWithDB(ctx context.Context, db *sql.DB, opts ...Option) (datastore.Datastore, TransactionBinder, error) {
	if db == nil {
		return nil, nil, errors.New("nil DB")
	}
	opts = append(opts, func(o *mysqlOptions) { o.borrowedDB = db; o.enablePrometheusStats = false })
	ds, err := newMySQLDatastore(ctx, "", primaryInstanceID, opts...)
	if err != nil {
		return nil, nil, err
	}
	ready, err := ds.ReadyState(ctx)
	if err != nil || !ready.IsReady {
		ds.Close()
		if err == nil {
			err = errors.New(ready.Message)
		}
		return nil, nil, err
	}
	id, err := ds.UniqueID(ctx)
	if err != nil {
		ds.Close()
		return nil, nil, err
	}
	return datastore.NewSeparatingContextDatastoreProxy(ds), &transactionBinder{ds: ds, identity: id}, nil
}

func (b *transactionBinder) WithTransaction(ctx context.Context, tx *sql.Tx, unified bool, fn datastore.TxUserFunc) (common.PendingTransaction, error) {
	if tx == nil || fn == nil {
		return nil, errors.New("transaction and callback are required")
	}
	var isolation, access, state string
	err := tx.QueryRowContext(ctx, `SELECT e.ISOLATION_LEVEL,e.ACCESS_MODE,e.STATE
 FROM performance_schema.events_transactions_current e
 JOIN performance_schema.threads t ON t.THREAD_ID=e.THREAD_ID
 WHERE t.PROCESSLIST_ID=CONNECTION_ID()`).Scan(&isolation, &access, &state)
	if err != nil {
		return nil, fmt.Errorf("cannot verify active transaction isolation: %w", err)
	}
	if isolation != "SERIALIZABLE" {
		return nil, fmt.Errorf("%w: got %s", common.ErrTransactionIsolation, isolation)
	}
	if access != "READ WRITE" || state != "ACTIVE" {
		return nil, common.ErrTransactionReadOnly
	}
	var id string
	if err := tx.QueryRowContext(ctx, "SELECT unique_id FROM "+b.ds.driver.Metadata()).Scan(&id); err != nil {
		return nil, err
	}
	if id != b.identity {
		return nil, common.ErrTransactionIdentity
	}
	adoptedTransactions.Lock()
	for key := range adoptedTransactions.bound {
		if key.Value() == nil {
			delete(adoptedTransactions.bound, key)
		}
	}
	_, exists := adoptedTransactions.bound[weak.Make(tx)]
	if !exists {
		adoptedTransactions.bound[weak.Make(tx)] = struct{}{}
	}
	adoptedTransactions.Unlock()
	if exists {
		return nil, common.ErrTransactionAlreadyBound
	}
	if unified {
		var hash []byte
		q := "SELECT hash FROM " + b.ds.driver.SchemaRevision() + " WHERE name='current' AND deleted_transaction=9223372036854775807 FOR SHARE"
		if err := tx.QueryRowContext(ctx, q).Scan(&hash); err != nil {
			return nil, err
		}
	}
	idNum, err := b.ds.createNewTransaction(ctx, tx, nil)
	if err != nil {
		return nil, err
	}
	if err := fn(ctx, b.ds.newReadWriteTransaction(tx, idNum)); err != nil {
		return nil, err
	}
	return common.NewPendingTransaction(func(ctx context.Context) (datastore.Revision, error) {
		defer b.forget(tx)
		if err := ctx.Err(); err != nil {
			return datastore.NoRevision, err
		}
		if err := tx.Commit(); err != nil {
			return datastore.NoRevision, err
		}
		return revisions.NewForTransactionID(idNum), nil
	}, func(context.Context) error { defer b.forget(tx); return tx.Rollback() }), nil
}

func (b *transactionBinder) forget(tx *sql.Tx) {
	adoptedTransactions.Lock()
	delete(adoptedTransactions.bound, weak.Make(tx))
	adoptedTransactions.Unlock()
}
