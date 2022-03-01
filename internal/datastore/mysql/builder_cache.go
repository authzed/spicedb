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
	CreateTxn        sq.InsertBuilder

	WriteNamespace        sq.InsertBuilder
	ReadNamespace         sq.SelectBuilder
	DeleteNamespace       sq.UpdateBuilder
	DeleteNamespaceTuples sq.UpdateBuilder

	QueryTuples      sq.SelectBuilder
	DeleteTuple      sq.UpdateBuilder
	QueryTupleExists sq.SelectBuilder
	WriteTuple       sq.InsertBuilder
	QueryChanged     sq.SelectBuilder
}

func NewBuilderCache(tablePrefix string) *BuilderCache {
	builder := BuilderCache{}

	builder.TableNamespace = TableNamespace(tablePrefix)
	builder.TableTransaction = TableTransaction(tablePrefix)
	builder.TableTuple = TableTuple(tablePrefix)

	// transaction builders
	builder.GetRevision = getRevision(builder.TableTransaction)
	builder.GetRevisionRange = getRevisionRange(builder.TableTransaction)
	builder.CreateTxn = createTxn(builder.TableTransaction)

	// namespace builders
	builder.WriteNamespace = writeNamespace(builder.TableNamespace)
	builder.ReadNamespace = readNamespace(builder.TableNamespace)
	builder.DeleteNamespace = deleteNamespace(builder.TableNamespace)

	// tuple builders
	builder.DeleteNamespaceTuples = deleteNamespaceTuples(builder.TableTuple)
	builder.QueryTuples = queryTuples(builder.TableTuple)
	builder.DeleteTuple = deleteTuple(builder.TableTuple)
	builder.QueryTupleExists = queryTupleExists(builder.TableTuple)
	builder.WriteTuple = writeTuple(builder.TableTuple)
	builder.QueryChanged = queryChanged(builder.TableTuple)

	return &builder
}

func TableNamespace(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableNamespaceDefault)
}

func TableTransaction(tablePrefix string) string {
	return fmt.Sprintf("%s%s", tablePrefix, common.TableTransactionDefault)
}

func TableTuple(tablePrefix string) string {
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
