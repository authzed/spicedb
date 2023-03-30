package common

import (
	"context"
	"fmt"
	"runtime"

	"github.com/authzed/spicedb/pkg/datastore"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type TupleQueryCursorer struct {
	QueryExecutor ExecuteQueryFunc
	Executor      ExecuteFunc
	FetchSize     uint
}

func (tqc TupleQueryCursorer) IteratorFor(ctx context.Context, filterer SchemaQueryFilterer) (datastore.RelationshipIterator, error) {
	sql, args, err := filterer.queryBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	// FIXME generate a random name that is valid for PGX cursor
	cursorName := "testtest"
	sql = fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR %s", cursorName, sql)
	err = tqc.Executor(ctx, sql, args)
	if err != nil {
		return nil, err
	}

	iter := NewCursorRelationshipIterator(ctx, tqc.QueryExecutor, cursorName, tqc.FetchSize)
	runtime.SetFinalizer(iter.(*cursorRelationshipIterator), MustCursorIteratorBeClosed)
	return iter, nil
}

// NewCursorRelationshipIterator creates a datastore.TupleIterator instance from a datastore-based cursor iterator.
func NewCursorRelationshipIterator(ctx context.Context, executor ExecuteQueryFunc, cursorName string, fetchSize uint) datastore.RelationshipIterator {
	return &cursorRelationshipIterator{ctx: ctx, executor: executor, cursorName: cursorName, query: fmt.Sprintf("FETCH %d FROM %s", fetchSize, cursorName)}
}

type cursorRelationshipIterator struct {
	ctx           context.Context
	executor      ExecuteQueryFunc
	fetchedTuples []*core.RelationTuple
	query         string
	cursorName    string
	fetchSize     uint
	closed        bool
	err           error
}

// Next implements TupleIterator
func (cri *cursorRelationshipIterator) Next() *core.RelationTuple {
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

func (cri *cursorRelationshipIterator) fetchTuples() {
	cri.fetchedTuples, cri.err = cri.executor(cri.ctx, cri.query, nil)
}

func (cri *cursorRelationshipIterator) closeCursor() {
	if cri.closed {
		return
	}
	_, cri.err = cri.executor(cri.ctx, "CLOSE "+cri.cursorName, nil)
}

func (cri *cursorRelationshipIterator) firstTupleFromBuffer() *core.RelationTuple {
	if len(cri.fetchedTuples) > 0 {
		tuple := cri.fetchedTuples[0]
		cri.fetchedTuples = cri.fetchedTuples[1:len(cri.fetchedTuples)]
		return tuple
	}
	return nil
}

// Err implements TupleIterator
func (cri *cursorRelationshipIterator) Err() error {
	return cri.err
}

// Close implements TupleIterator
func (cri *cursorRelationshipIterator) Close() {
	if cri.closed {
		return
	}

	cri.closeCursor()
	cri.closed = true
}

// MustCursorIteratorBeClosed
func MustCursorIteratorBeClosed(iter *cursorRelationshipIterator) {
	if !iter.closed {
		panic("Cursor tuple iterator garbage collected before Close() was called")
	}
}
