package mysql

import (
	"context"
	"database/sql"

	"github.com/authzed/spicedb/internal/datastore/common"
)

func (mds *mysqlDatastore) newReadWriteTransaction(tx *sql.Tx, newTxnID uint64) *mysqlReadWriteTXN {
	longLivedTx := func(context.Context) (*sql.Tx, txCleanupFunc, error) {
		return tx, noCleanup, nil
	}

	executor := common.QueryRelationshipsExecutor{
		Executor: newMySQLExecutor(tx, mds),
	}

	return &mysqlReadWriteTXN{
		&mysqlReader{
			mds.QueryBuilder,
			longLivedTx,
			executor,
			currentlyLivingObjects,
			mds.filterMaximumIDCount,
			mds.schema,
		},
		mds.driver.RelationTuple(),
		mds.driver.SchemaRevision(),
		tx,
		newTxnID,
		false,
	}
}
