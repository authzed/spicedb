package postgres

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var _ datastore.CreatedRelationshipsReader = (*pgDatastore)(nil)

// RelationshipsCreatedAfter implements datastore.CreatedRelationshipsReader.
//
// The querier is a reader on the write pool, so that it reads the primary outside any
// transaction, whose rows are the live relationships created after the revision instead of
// those alive at a snapshot. Its queries stream their results and apply their filters and limits
// in SQL, like those of any other reader.
func (pgd *pgDatastore) RelationshipsCreatedAfter(revisionRaw datastore.Revision) (datastore.RelationshipQuerier, error) {
	revision, ok := revisionRaw.(postgresRevision)
	if !ok {
		return nil, spiceerrors.MustBugf("unexpected revision type %T", revisionRaw)
	}

	queryFuncs := pgxcommon.QuerierFuncsFor(pgd.writePool)
	executor := common.QueryRelationshipsExecutor{
		Executor: pgxcommon.NewPGXQueryRelationshipsExecutor(queryFuncs, pgd),
	}

	return &pgReader{
		queryFuncs,
		executor,
		buildCreatedAfterFilter(revision),
		pgd.filterMaximumIDCount,
		pgd.schema,
	}, nil
}

// buildCreatedAfterFilter returns a filter for the live relationships created by transactions
// that revision does not include.
func buildCreatedAfterFilter(revision postgresRevision) queryFilterer {
	createdAfterTXN := sq.Expr(fmt.Sprintf(
		snapshotAlive,
		schema.ColCreatedXid,
	), revision.snapshot, false)

	return func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.
			Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID}).
			// Redundant with the next condition, since the revision includes every transaction
			// below its xmin, but it lets the planner bound the rows by ix_watch_api_index.
			Where(sq.GtOrEq{schema.ColCreatedXid: revision.snapshot.xmin}).
			Where(createdAfterTXN)
	}
}
