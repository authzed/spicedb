package crdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (cds *crdbDatastore) newReadWriteTransaction(ctx context.Context, tx pgx.Tx) *crdbReadWriteTXN {
	querier := pgxcommon.QuerierFuncsFor(tx)
	executor := common.QueryRelationshipsExecutor{
		Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(querier, cds),
	}

	reader := &crdbReader{
		schema:               cds.schema,
		query:                querier,
		executor:             executor,
		keyer:                cds.writeOverlapKeyer,
		overlapKeySet:        cds.overlapKeyInit(ctx),
		filterMaximumIDCount: cds.filterMaximumIDCount,
		withIntegrity:        cds.supportsIntegrity,
		atSpecificRevision:   "", // No AS OF SYSTEM TIME for writes
	}

	return &crdbReadWriteTXN{
		reader,
		tx,
		0,
	}
}

func (cds *crdbDatastore) finalizeTransaction(ctx context.Context, rwt *crdbReadWriteTXN, metadata map[string]any, skipRevision bool) (datastore.Revision, error) {
	// If the user supplied transaction metadata, write it to the metadata
	// table so the Watch API can attach it to the revision's changes.
	tx := rwt.tx
	if len(metadata) > 0 {
		expiresAt := time.Now().Add(cds.gcWindow).Add(1 * time.Minute)
		insertTransactionMetadata := psql.Insert(schema.TableTransactionMetadata).
			Columns(schema.ColExpiresAt, schema.ColMetadata).
			Values(expiresAt, metadata)

		sql, args, err := insertTransactionMetadata.ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf("error building metadata insert: %w", err)
		}

		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			return datastore.NoRevision, fmt.Errorf("error writing metadata: %w", err)
		}
	}

	// Touching the transaction key happens last so that the "write intent" for
	// the transaction as a whole lands in a range for the affected tuples.
	for k := range rwt.overlapKeySet {
		if _, err := tx.Exec(ctx, queryTouchTransaction, k); err != nil {
			return datastore.NoRevision, fmt.Errorf("error writing overlapping keys: %w", err)
		}
	}

	// Reading the commit revision costs a separate SHOW COMMIT TIMESTAMP
	// round trip. Callers that discard the revision (e.g. bulk deletion)
	// skip it; the transaction still commits normally via tx.Commit and
	// this returns NoRevision.
	if skipRevision {
		return datastore.NoRevision, nil
	}

	commitTimestamp, cerr := cds.readTransactionCommitRev(ctx, pgxcommon.QuerierFuncsFor(tx))
	if cerr != nil {
		return datastore.NoRevision, fmt.Errorf("error getting commit timestamp: %w", cerr)
	}
	return commitTimestamp, nil
}
