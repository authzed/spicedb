package mysql

import (
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"

	sq "github.com/Masterminds/squirrel"
)

type QueryBuilder struct {
	GetRevision      sq.SelectBuilder
	GetRevisionRange sq.SelectBuilder

	WriteNamespaceQuery        sq.InsertBuilder
	ReadNamespaceQuery         sq.SelectBuilder
	DeleteNamespaceQuery       sq.UpdateBuilder
	DeleteNamespaceTuplesQuery sq.UpdateBuilder

	QueryTupleIdsQuery    sq.SelectBuilder
	QueryTuplesQuery      sq.SelectBuilder
	DeleteTupleQuery      sq.UpdateBuilder
	QueryTupleExistsQuery sq.SelectBuilder
	WriteTupleQuery       sq.InsertBuilder
	QueryChangedQuery     sq.SelectBuilder
}

func NewQueryBuilder(driver *migrations.MySQLDriver) *QueryBuilder {
	builder := QueryBuilder{}

	// transaction builders
	builder.GetRevision = getRevision(driver.RelationTupleTransaction())
	builder.GetRevisionRange = getRevisionRange(driver.RelationTupleTransaction())

	// namespace builders
	builder.WriteNamespaceQuery = writeNamespace(driver.Namespace())
	builder.ReadNamespaceQuery = readNamespace(driver.Namespace())
	builder.DeleteNamespaceQuery = deleteNamespace(driver.Namespace())

	// tuple builders
	builder.QueryTupleIdsQuery = queryTupleIds(driver.RelationTuple())
	builder.DeleteNamespaceTuplesQuery = deleteNamespaceTuples(driver.RelationTuple())
	builder.QueryTuplesQuery = queryTuples(driver.RelationTuple())
	builder.DeleteTupleQuery = deleteTuple(driver.RelationTuple())
	builder.QueryTupleExistsQuery = queryTupleExists(driver.RelationTuple())
	builder.WriteTupleQuery = writeTuple(driver.RelationTuple())
	builder.QueryChangedQuery = queryChanged(driver.RelationTuple())

	return &builder
}

func getRevision(tableTransaction string) sq.SelectBuilder {
	return sb.Select("MAX(id)").From(tableTransaction).Limit(1)
}

func getRevisionRange(tableTransaction string) sq.SelectBuilder {
	return sb.Select("MIN(id)", "MAX(id)").From(tableTransaction)
}

func writeNamespace(tableNamespace string) sq.InsertBuilder {
	return sb.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
		colCreatedTxn,
	)
}

func readNamespace(tableNamespace string) sq.SelectBuilder {
	return sb.Select(colConfig, colCreatedTxn).From(tableNamespace)
}

func deleteNamespace(tableNamespace string) sq.UpdateBuilder {
	return sb.Update(tableNamespace).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
}

func deleteNamespaceTuples(tableTuple string) sq.UpdateBuilder {
	return sb.Update(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
}

func queryTupleIds(tableTuple string) sq.SelectBuilder {
	return sb.Select(
		colID,
	).From(tableTuple)
}

func queryTuples(tableTuple string) sq.SelectBuilder {
	return sb.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).From(tableTuple)
}

func deleteTuple(tableTuple string) sq.UpdateBuilder {
	return sb.Update(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
}

func queryTupleExists(tableTuple string) sq.SelectBuilder {
	return sb.Select(colID).From(tableTuple)
}

func writeTuple(tableTuple string) sq.InsertBuilder {
	return sb.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedTxn,
	)
}

func queryChanged(tableTuple string) sq.SelectBuilder {
	return sb.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedTxn,
		colDeletedTxn,
	).From(tableTuple)
}
