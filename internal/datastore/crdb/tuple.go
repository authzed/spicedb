package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToWriteTuples     = "unable to write tuples: %w"
	errUnableToVerifyNamespace = "unable to verify namespace: %w"
	errUnableToVerifyRelation  = "unable to verify relation: %w"
)

var (
	upsertTupleSuffix = fmt.Sprintf(
		"ON CONFLICT (%s,%s,%s,%s,%s,%s) DO UPDATE SET %s = now() %s",
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colTimestamp,
		queryReturningTimestamp,
	)
	queryWriteTuple = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).Suffix(upsertTupleSuffix)

	queryDeleteTuples = psql.Delete(tableTuple)

	queryTupleExists = psql.Select(colObjectID).From(tableTuple)
)

func (cds *crdbDatastore) WriteTuples(ctx context.Context, preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (datastore.Revision, error) {
	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback(ctx)

	// Check the preconditions
	for _, tpl := range preconditions {
		sql, args, err := queryTupleExists.Where(exactTupleClause(tpl)).Limit(1).ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}

		var foundObjectID string
		if err := tx.QueryRow(ctx, sql, args...).Scan(&foundObjectID); err != nil {
			if err == pgx.ErrNoRows {
				return datastore.NoRevision, datastore.ErrPreconditionFailed
			}
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	}

	bulkWrite := queryWriteTuple
	var bulkWriteCount int64

	// Process the actual updates
	for _, mutation := range mutations {
		tpl := mutation.Tuple

		if mutation.Operation == pb.RelationTupleUpdate_DELETE {
			sql, args, err := queryDeleteTuples.Where(exactTupleClause(tpl)).ToSql()
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}

			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}
		}

		if mutation.Operation == pb.RelationTupleUpdate_TOUCH || mutation.Operation == pb.RelationTupleUpdate_CREATE {
			bulkWrite = bulkWrite.Values(
				tpl.ObjectAndRelation.Namespace,
				tpl.ObjectAndRelation.ObjectId,
				tpl.ObjectAndRelation.Relation,
				tpl.User.GetUserset().Namespace,
				tpl.User.GetUserset().ObjectId,
				tpl.User.GetUserset().Relation,
			)
			bulkWriteCount++
		}
	}

	var nowRevision decimal.Decimal
	if bulkWriteCount > 0 {
		sql, args, err := bulkWrite.ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}

		if err := tx.QueryRow(ctx, sql, args...).Scan(&nowRevision); err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	} else {
		nowRevision, err = readCRDBNow(ctx, tx)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return nowRevision, nil
}

func exactTupleClause(tpl *pb.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        tpl.ObjectAndRelation.Namespace,
		colObjectID:         tpl.ObjectAndRelation.ObjectId,
		colRelation:         tpl.ObjectAndRelation.Relation,
		colUsersetNamespace: tpl.User.GetUserset().Namespace,
		colUsersetObjectID:  tpl.User.GetUserset().ObjectId,
		colUsersetRelation:  tpl.User.GetUserset().Relation,
	}
}
