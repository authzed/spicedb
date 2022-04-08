package mysql

// this file is stuff that was generalized into common package
// in an attempt to reduce the duplication between psql and mysql
//
// in order to make the merge-upstream a tractable
// problem, this file captures things that
// we attempted to generalize into the common package
// and should be considered after our fork has been
// reconciled with upstream

import (
	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/jzelinskie/stringz"
	"github.com/shopspring/decimal"
)

const (
	colID               = "id"
	colTimestamp        = "timestamp"
	colNamespace        = "namespace"
	colConfig           = "serialized_config"
	colCreatedTxn       = "created_transaction"
	colDeletedTxn       = "deleted_transaction"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"
)

var (
	ErrUnableToQueryTuples  = "unable to query tuples: %w"
	ErrUnableToWriteTuples  = "unable to write tuples: %w"
	ErrUnableToDeleteTuples = "unable to delete tuples: %w"

	ErrUnableToWriteConfig    = "unable to write namespace config: %w"
	ErrUnableToReadConfig     = "unable to read namespace config: %w"
	ErrUnableToDeleteConfig   = "unable to delete namespace config: %w"
	ErrUnableToListNamespaces = "unable to list namespaces: %w"
)

func FilterToLivingObjects(original sq.SelectBuilder, revision datastore.Revision) sq.SelectBuilder {
	return original.Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
		Where(sq.Or{
			sq.Eq{colDeletedTxn: liveDeletedTxnID},
			sq.Gt{colDeletedTxn: revision},
		})
}

func transactionFromRevision(revision datastore.Revision) uint64 {
	return uint64(revision.IntPart())
}

func revisionFromTransaction(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}

func exactRelationshipClause(r *v1.Relationship) sq.Eq {
	return sq.Eq{
		colNamespace:        r.Resource.ObjectType,
		colObjectID:         r.Resource.ObjectId,
		colRelation:         r.Relation,
		colUsersetNamespace: r.Subject.Object.ObjectType,
		colUsersetObjectID:  r.Subject.Object.ObjectId,
		colUsersetRelation:  stringz.DefaultEmpty(r.Subject.OptionalRelation, datastore.Ellipsis),
	}
}
