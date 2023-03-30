package common

import (
	"context"
	"runtime"

	"github.com/authzed/spicedb/pkg/datastore"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type PaginatedTupleQuery struct {
	Executor  ExecuteQueryFunc
	FetchSize uint64
}

func (ctq PaginatedTupleQuery) IteratorFor(ctx context.Context, filterer SchemaQueryFilterer, relFilter datastore.RelationshipsFilter) (datastore.RelationshipIterator, error) {
	iter := newPaginatedRelationshipIterator(ctx, filterer, relFilter, ctq.Executor, ctq.FetchSize)
	runtime.SetFinalizer(iter.(*paginatedRelationshipIterator), MustCursorIteratorBeClosed)
	return iter, nil
}

func newPaginatedRelationshipIterator(ctx context.Context, schemaFilter SchemaQueryFilterer, relFilter datastore.RelationshipsFilter, executor ExecuteQueryFunc, fetchSize uint64) datastore.RelationshipIterator {
	return &paginatedRelationshipIterator{ctx: ctx, executor: executor, schemaFilterer: schemaFilter, relFilter: relFilter, fetchSize: fetchSize}
}

type paginatedRelationshipIterator struct {
	ctx            context.Context
	executor       ExecuteQueryFunc
	fetchedTuples  []*core.RelationTuple
	schemaFilterer SchemaQueryFilterer
	relFilter      datastore.RelationshipsFilter
	cursor         *core.RelationTuple
	fetchSize      uint64
	closed         bool
	err            error
}

// Next implements TupleIterator
func (cri *paginatedRelationshipIterator) Next() *core.RelationTuple {
	if cri.err != nil {
		return nil
	}
	if tuple := cri.firstTupleFromBuffer(); tuple != nil {
		return tuple
	}

	cri.fetchTuples()
	if cri.err != nil {
		return nil
	}

	return cri.firstTupleFromBuffer()
}

func (cri *paginatedRelationshipIterator) fetchTuples() {
	filterer := cri.schemaFilterer
	if cri.cursor != nil {
		filterer = filterer.CursorFromTuple(cri.cursor)
	} else {
		filterer, cri.err = filterer.FilterWithRelationshipsFilter(cri.relFilter)
		if cri.err != nil {
			return
		}
	}
	sql, args, err := filterer.limit(cri.fetchSize).TupleOrder().queryBuilder.ToSql()
	if err != nil {
		cri.err = err
		return
	}

	cri.fetchedTuples, cri.err = cri.executor(cri.ctx, sql, args)
}

func (cri *paginatedRelationshipIterator) firstTupleFromBuffer() *core.RelationTuple {
	if len(cri.fetchedTuples) > 0 {
		tuple := cri.fetchedTuples[0]
		cri.fetchedTuples = cri.fetchedTuples[1:len(cri.fetchedTuples)]
		cri.cursor = tuple
		return tuple
	}
	return nil
}

// Err implements TupleIterator
func (cri *paginatedRelationshipIterator) Err() error {
	if err := cri.ctx.Err(); err != nil {
		return err
	}
	return cri.err
}

// Close implements TupleIterator
func (cri *paginatedRelationshipIterator) Close() {
	if cri.closed {
		return
	}

	cri.closed = true
}

// MustCursorIteratorBeClosed
func MustCursorIteratorBeClosed(iter *paginatedRelationshipIterator) {
	if !iter.closed {
		panic("Cursor tuple iterator garbage collected before Close() was called")
	}
}
