package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

// ExternalTransactionOptions controls transaction-local schema validation.
type ExternalTransactionOptions struct{ LockUnifiedSchema bool }

// TransactionBinder stages a single callback in an existing transaction.
type TransactionBinder interface {
	WithTransaction(context.Context, pgx.Tx, ExternalTransactionOptions, datastore.TxUserFunc) (common.PendingTransaction, error)
}
type transactionBinder struct {
	pg       *pgDatastore
	identity string
}

func (b *transactionBinder) WithTransaction(ctx context.Context, tx pgx.Tx, opts ExternalTransactionOptions, fn datastore.TxUserFunc) (common.PendingTransaction, error) {
	if tx == nil || fn == nil {
		return nil, errors.New("transaction and callback are required")
	}
	if strings.Contains(fmt.Sprintf("%T", tx), "dbSimulatedNestedTx") {
		return nil, errors.New("nested transactions cannot be adopted")
	}
	var isolation, access string
	if err := tx.QueryRow(ctx, "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')").Scan(&isolation, &access); err != nil {
		return nil, err
	}
	if isolation != "serializable" {
		return nil, fmt.Errorf("%w: got %s", common.ErrTransactionIsolation, isolation)
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
	var bound bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM relation_tuple_transaction WHERE xid = pg_current_xact_id())").Scan(&bound); err != nil {
		return nil, err
	}
	if bound {
		return nil, common.ErrTransactionAlreadyBound
	}
	if opts.LockUnifiedSchema {
		var hash []byte
		if err := tx.QueryRow(ctx, "SELECT hash FROM schema_revision WHERE name='current' AND deleted_xid='9223372036854775807'::xid8 FOR SHARE").Scan(&hash); err != nil {
			return nil, err
		}
	}
	xid, snapshot, timestamp, err := createNewTransaction(ctx, tx, nil)
	if err != nil {
		return nil, err
	}
	writer := b.pg.newReadWriteTransaction(tx, xid)
	if err := fn(ctx, writer); err != nil {
		return nil, err
	}
	return common.NewPendingTransaction(func(ctx context.Context) (datastore.Revision, error) {
		rev, err := committedRevision(xid, snapshot, timestamp)
		if err != nil {
			return datastore.NoRevision, err
		}
		if err := tx.Commit(ctx); err != nil {
			return datastore.NoRevision, err
		}
		return rev, nil
	}, tx.Rollback), nil
}

func (pgd *pgDatastore) newReadWriteTransaction(tx pgx.Tx, xid xid8) *pgReadWriteTXN {
	q := pgxcommon.QuerierFuncsFor(tx)
	return &pgReadWriteTXN{&pgReader{q, common.QueryRelationshipsExecutor{Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(q, pgd)}, currentlyLivingObjects, pgd.filterMaximumIDCount, pgd.schema}, tx, xid, false}
}

func committedRevision(xid xid8, snapshot pgSnapshot, timestamp time.Time) (datastore.Revision, error) {
	nanos, err := safecast.Convert[uint64](timestamp.UnixNano())
	if err != nil {
		return datastore.NoRevision, err
	}
	return postgresRevision{snapshot: snapshot.markComplete(xid.Uint64), optionalTxID: xid, optionalInexactNanosTimestamp: nanos}, nil
}
