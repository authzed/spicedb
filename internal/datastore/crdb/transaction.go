package crdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TransactionBinder stages writes using a caller's transaction.
type TransactionBinder interface {
	WithTransaction(context.Context, pgx.Tx, datastore.TxUserFunc) (common.PendingTransaction, error)
}
type transactionBinder struct {
	ds       *crdbDatastore
	identity string
}

// NewCRDBDatastoreWithPool borrows an application pool without taking ownership.
func NewCRDBDatastoreWithPool(ctx context.Context, p *pgxpool.Pool, opts ...Option) (datastore.Datastore, TransactionBinder, error) {
	if p == nil {
		return nil, nil, errors.New("nil pool")
	}
	opts = append(opts, func(o *crdbOptions) {
		o.borrowedPool = p
		o.enableConnectionBalancing = false
		o.enablePrometheusStats = false
	})
	ds, err := newCRDBDatastore(ctx, "", opts...)
	if err != nil {
		return nil, nil, err
	}
	cds := ds.(*crdbDatastore)
	ready, err := cds.ReadyState(ctx)
	if err != nil || !ready.IsReady {
		cds.Close()
		if err == nil {
			err = errors.New(ready.Message)
		}
		return nil, nil, err
	}
	id, err := cds.UniqueID(ctx)
	if err != nil {
		cds.Close()
		return nil, nil, err
	}
	return datastore.NewSeparatingContextDatastoreProxy(cds), &transactionBinder{ds: cds, identity: id}, nil
}

func (b *transactionBinder) WithTransaction(ctx context.Context, tx pgx.Tx, fn datastore.TxUserFunc) (common.PendingTransaction, error) {
	if tx == nil || fn == nil {
		return nil, errors.New("transaction and callback are required")
	}
	if strings.Contains(fmt.Sprintf("%T", tx), "dbSimulatedNestedTx") {
		return nil, errors.New("nested transactions cannot be adopted")
	}
	var isolation, access string
	if err := tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
		return nil, err
	}
	if !strings.EqualFold(isolation, "serializable") {
		return nil, fmt.Errorf("%w: got %s", common.ErrTransactionIsolation, isolation)
	}
	if err := tx.QueryRow(ctx, "SHOW transaction_read_only").Scan(&access); err != nil {
		return nil, err
	}
	if access != "off" {
		return nil, common.ErrTransactionReadOnly
	}
	var id string
	if err := tx.QueryRow(ctx, "SELECT unique_id FROM metadata").Scan(&id); err != nil {
		return nil, err
	}
	if id != b.identity {
		return nil, common.ErrTransactionIdentity
	}
	RegisterTypes(tx.Conn().TypeMap())

	binding, err := b.reserveTransaction(ctx, tx)
	if err != nil {
		return nil, err
	}
	retained := false
	defer func() {
		if !retained {
			b.forget(binding)
		}
	}()

	rwt := b.ds.newReadWriteTransaction(ctx, tx)
	if err := fn(ctx, rwt); err != nil {
		return nil, err
	}
	retained = true
	return common.NewPendingTransaction(func(ctx context.Context) (datastore.Revision, error) {
		defer b.forget(binding)
		rev, err := b.ds.finalizeTransaction(ctx, rwt, nil, false)
		if err != nil {
			return datastore.NoRevision, err
		}
		if err := binding.tx.Commit(ctx); err != nil {
			return datastore.NoRevision, err
		}
		return rev, nil
	}, func(ctx context.Context) error { defer b.forget(binding); return binding.tx.Rollback(ctx) }), nil
}
