package mysql

import (
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"

	sq "github.com/Masterminds/squirrel"
)

// QueryBuilder captures all parameterizable queries used
// by the MySQL datastore implementation
type QueryBuilder struct {
	GetLastRevision  sq.SelectBuilder
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
	CountTupleQuery       sq.SelectBuilder
}

// NewQueryBuilder returns a new QueryBuilder instance. The migration
// driver is used to determine the names of the tables.
func NewQueryBuilder(driver *migrations.MySQLDriver) *QueryBuilder {
	builder := QueryBuilder{}

	// transaction builders
	builder.GetLastRevision = getLastRevision(driver.RelationTupleTransaction())
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
	builder.CountTupleQuery = countTuples(driver.RelationTuple())

	return &builder
}

func getLastRevision(tableTransaction string) sq.SelectBuilder {
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

func countTuples(tableTuple string) sq.SelectBuilder {
	return sb.Select(
		"count(*)",
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
