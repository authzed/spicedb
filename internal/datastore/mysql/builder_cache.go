package mysql

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
)

type BuilderCache struct {
	GetRevision      sq.SelectBuilder
	GetRevisionRange sq.SelectBuilder

	WriteNamespaceQuery        sq.InsertBuilder
	ReadNamespaceQuery         sq.SelectBuilder
	DeleteNamespaceQuery       sq.UpdateBuilder
	DeleteNamespaceTuplesQuery sq.UpdateBuilder

	QueryTuplesQuery      sq.SelectBuilder
	DeleteTupleQuery      sq.UpdateBuilder
	QueryTupleExistsQuery sq.SelectBuilder
	WriteTupleQuery       sq.InsertBuilder
	QueryChangedQuery     sq.SelectBuilder
}

func NewBuilderCache(tableTransaction, tableNamespace, tableTuple string) *BuilderCache {
	builder := BuilderCache{}

	// transaction builders
	builder.GetRevision = getRevision(tableTransaction)
	builder.GetRevisionRange = getRevisionRange(tableTransaction)

	// namespace builders
	builder.WriteNamespaceQuery = writeNamespace(tableNamespace)
	builder.ReadNamespaceQuery = readNamespace(tableNamespace)
	builder.DeleteNamespaceQuery = deleteNamespace(tableNamespace)

	// tuple builders
	builder.DeleteNamespaceTuplesQuery = deleteNamespaceTuples(tableTuple)
	builder.QueryTuplesQuery = queryTuples(tableTuple)
	builder.DeleteTupleQuery = deleteTuple(tableTuple)
	builder.QueryTupleExistsQuery = queryTupleExists(tableTuple)
	builder.WriteTupleQuery = writeTuple(tableTuple)
	builder.QueryChangedQuery = queryChanged(tableTuple)

	return &builder
}

func getRevision(tableTransaction string) sq.SelectBuilder {
	return sb.Select("MAX(id)").From(tableTransaction)
}

func getRevisionRange(tableTransaction string) sq.SelectBuilder {
	return sb.Select("MIN(id)", "MAX(id)").From(tableTransaction)
}

func writeNamespace(tableNamespace string) sq.InsertBuilder {
	return sb.Insert(tableNamespace).Columns(
		common.ColNamespace,
		common.ColConfig,
		common.ColCreatedTxn,
	)
}

func readNamespace(tableNamespace string) sq.SelectBuilder {
	return sb.Select(common.ColConfig, common.ColCreatedTxn).From(tableNamespace)
}

func deleteNamespace(tableNamespace string) sq.UpdateBuilder {
	return sb.Update(tableNamespace).Where(sq.Eq{common.ColDeletedTxn: liveDeletedTxnID})
}

func deleteNamespaceTuples(tableTuple string) sq.UpdateBuilder {
	return sb.Update(tableTuple).Where(sq.Eq{common.ColDeletedTxn: liveDeletedTxnID})
}

func queryTuples(tableTuple string) sq.SelectBuilder {
	return sb.Select(
		common.ColNamespace,
		common.ColObjectID,
		common.ColRelation,
		common.ColUsersetNamespace,
		common.ColUsersetObjectID,
		common.ColUsersetRelation,
	).From(tableTuple)
}

func deleteTuple(tableTuple string) sq.UpdateBuilder {
	return sb.Update(tableTuple).Where(sq.Eq{common.ColDeletedTxn: liveDeletedTxnID})
}

func queryTupleExists(tableTuple string) sq.SelectBuilder {
	return sb.Select(common.ColID).From(tableTuple)
}

func writeTuple(tableTuple string) sq.InsertBuilder {
	return sb.Insert(tableTuple).Columns(
		common.ColNamespace,
		common.ColObjectID,
		common.ColRelation,
		common.ColUsersetNamespace,
		common.ColUsersetObjectID,
		common.ColUsersetRelation,
		common.ColCreatedTxn,
	)
}

func queryChanged(tableTuple string) sq.SelectBuilder {
	return sb.Select(
		common.ColNamespace,
		common.ColObjectID,
		common.ColRelation,
		common.ColUsersetNamespace,
		common.ColUsersetObjectID,
		common.ColUsersetRelation,
		common.ColCreatedTxn,
		common.ColDeletedTxn,
	).From(tableTuple)
}
