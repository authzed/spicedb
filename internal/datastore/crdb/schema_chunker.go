package crdb

import (
	"context"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
)

const (
	// CockroachDB has no practical limit on BYTEA column size (similar to Postgres),
	// but we use 1MB chunks for reasonable memory usage and query performance.
	crdbMaxChunkSize = 1024 * 1024 // 1MB
)

// binaryChunkTransfer forces pgx to fetch the statement description before
// executing, so that bytea parameters and results use the binary format.
//
// It exists to override the READ pool's connection default. newCRDBDatastore puts
// the read pool on pgx.QueryExecModeExec and leaves the write pool on
// cache_statement, so this is load-bearing for chunk READS and belt-and-braces for
// chunk WRITES, which already get the binary format from the write pool's mode.
// It is applied on both paths so behaviour does not silently depend on which pool
// a caller happens to be using.
//
// Under exec, parameters and results are text, and bytea in text is hex-encoded,
// so every chunk doubles in size on the wire. That is merely wasteful on most
// queries, but it breaks
// schema writes outright: WriteChunkedBytes puts every chunk of a schema into a
// single multi-row INSERT, so the pgwire message is the size of the whole
// serialized schema. Doubling it pushes a large schema past CockroachDB's 16MiB
// message limit -- observed as
//
//	failed to insert chunks: message size 17 MiB bigger than maximum allowed
//	message size 16 MiB (SQLSTATE 08P01)
//
// which in effect halves the largest schema SpiceDB can store on CRDB. Note that
// shrinking crdbMaxChunkSize does not help, because the limit is hit by the
// combined message rather than by any individual chunk.
//
// DescribeExec is used rather than CacheStatement deliberately. Both restore the
// binary format, but CacheStatement would leave named prepared statements on the
// server, which is the very cost the exec-mode default was introduced to avoid;
// and because the INSERT's SQL text varies with the number of chunks, caching it
// would churn rather than be reused. DescribeExec costs one extra round trip, on
// queries that run only when a schema is written or when the schema cache misses.
const binaryChunkTransfer = pgx.QueryExecModeDescribeExec

// BaseSchemaChunkerConfig provides the base configuration for CRDB schema chunking.
// CRDB uses delete-and-insert write mode since it handles MVCC automatically.
var BaseSchemaChunkerConfig = common.SQLByteChunkerConfig[any]{
	TableName:         "schema",
	NameColumn:        "name",
	ChunkIndexColumn:  "chunk_index",
	ChunkDataColumn:   "chunk_data",
	MaxChunkSize:      crdbMaxChunkSize,
	PlaceholderFormat: sq.Dollar,
	WriteMode:         common.WriteModeDeleteAndInsert,
}

// revisionAwareExecutor wraps the reader's query infrastructure to provide revision-aware chunk reading
type revisionAwareExecutor struct {
	query             pgxcommon.DBFuncQuerier
	addFromToQuery    func(sq.SelectBuilder, string, string) sq.SelectBuilder
	assertAsOfSysTime func(string)
}

func (e *revisionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// We don't support transactions for reading
	return nil, errors.New("transactions not supported for revision-aware reads")
}

func (e *revisionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	// Modify the builder to add AS OF SYSTEM TIME
	builder = e.addFromToQuery(builder, "schema", "")

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}
	e.assertAsOfSysTime(sql)

	// Execute using the reader's query function
	result := make(map[int][]byte)
	err = e.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		defer rows.Close()
		for rows.Next() {
			var chunkIndex int
			var chunkData []byte
			if err := rows.Scan(&chunkIndex, &chunkData); err != nil {
				return err
			}
			result[chunkIndex] = chunkData
		}
		return rows.Err()
		// binaryChunkTransfer: chunk_data is bytea, which the pool's default text
		// mode would return hex-encoded at twice the wire size.
	}, sql, append([]any{binaryChunkTransfer}, args...)...)

	return result, err
}

// transactionAwareExecutor wraps an existing pgx.Tx to provide transaction-aware chunk writing
type transactionAwareExecutor struct {
	tx pgx.Tx
}

func newTransactionAwareExecutor(tx pgx.Tx) *transactionAwareExecutor {
	return &transactionAwareExecutor{tx: tx}
}

func (e *transactionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// Return a transaction wrapper that uses the existing transaction
	return &transactionAwareTransaction{tx: e.tx}, nil
}

func (e *transactionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	return nil, errors.New("read operations not supported on transaction-aware executor")
}

// transactionAwareTransaction implements common.ChunkedBytesTransaction using an existing pgx.Tx
type transactionAwareTransaction struct {
	tx pgx.Tx
}

func (t *transactionAwareTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	// binaryChunkTransfer: this INSERT carries every chunk of the schema as bytea
	// parameters in one message. Under the pool's default text mode they would be
	// hex-encoded, doubling the message and breaching CRDB's 16MiB pgwire limit for
	// large schemas.
	_, err = t.tx.Exec(ctx, sql, append([]any{binaryChunkTransfer}, args...)...)
	return err
}

func (t *transactionAwareTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}

func (t *transactionAwareTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}
