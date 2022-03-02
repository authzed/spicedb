package mysql

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
)

type BuilderCache struct {
	TableNamespace   string
	TableTransaction string
	TableTuple       string

	GetRevision      sq.SelectBuilder
	GetRevisionRange sq.SelectBuilder
	CreateTxnQuery   sq.InsertBuilder

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

func NewBuilderCache(tablePrefix string) *BuilderCache {
	builder := BuilderCache{}

	builder.TableNamespace = tableNamespace(tablePrefix)
	builder.TableTransaction = tableTransaction(tablePrefix)
	builder.TableTuple = tableTuple(tablePrefix)

	// transaction builders
	builder.GetRevision = getRevision(builder.TableTransaction)
	builder.GetRevisionRange = getRevisionRange(builder.TableTransaction)
	builder.CreateTxnQuery = createTxn(builder.TableTransaction)

	// namespace builders
	builder.WriteNamespaceQuery = writeNamespace(builder.TableNamespace)
	builder.ReadNamespaceQuery = readNamespace(builder.TableNamespace)
	builder.DeleteNamespaceQuery = deleteNamespace(builder.TableNamespace)

	// tuple builders
	builder.DeleteNamespaceTuplesQuery = deleteNamespaceTuples(builder.TableTuple)
	builder.QueryTuplesQuery = queryTuples(builder.TableTuple)
	builder.DeleteTupleQuery = deleteTuple(builder.TableTuple)
	builder.QueryTupleExistsQuery = queryTupleExists(builder.TableTuple)
	builder.WriteTupleQuery = writeTuple(builder.TableTuple)
	builder.QueryChangedQuery = queryChanged(builder.TableTuple)

	return &builder
}

func tableNamespace(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableNamespaceDefault)
}

func tableTransaction(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableTransactionDefault)
}

func tableTuple(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableTupleDefault)
}

func getRevision(tableTransaction string) sq.SelectBuilder {
	return sb.Select("MAX(id)").From(tableTransaction)
}

func getRevisionRange(tableTransaction string) sq.SelectBuilder {
	return sb.Select("MIN(id)", "MAX(id)").From(tableTransaction)
}

func createTxn(tableTransaction string) sq.InsertBuilder {
	return sb.Insert(tableTransaction).Values()
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
