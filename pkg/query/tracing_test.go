package query

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestIteratorTracing(t *testing.T) {
	// Create a test context with TraceLogger
	traceLogger := NewTraceLogger()

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(t, err)

	ctx := &Context{
		Context:     context.Background(),
		Executor:    LocalExecutor{},
		Reader:      ds.SnapshotReader(revision),
		TraceLogger: traceLogger,
	}

	// Create test paths
	testPath1 := MustPathFromString("document:doc1#view@user:alice")
	testPath2 := MustPathFromString("document:doc2#view@user:bob")

	// Test FixedIterator tracing
	fixedIter := NewFixedIterator(testPath1, testPath2)

	// Test CheckImpl tracing
	resources := []Object{NewObject("document", "doc1")}
	subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

	seq, err := ctx.Check(fixedIter, resources, subject)
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	// Verify tracing output
	trace := traceLogger.DumpTrace()
	require.True(t, strings.Contains(trace, "-> Fixed: check(document:doc1, user:alice)"))
	require.True(t, strings.Contains(trace, "<- Fixed: returned 1 paths"))
	require.True(t, strings.Contains(trace, "Fixed: checking 2 paths against 1 resources"))
	require.True(t, strings.Contains(trace, "Fixed: found 1 matching paths"))

	t.Run("IterSubjects tracing", func(t *testing.T) {
		// Reset trace logger
		traceLogger = NewTraceLogger()
		ctx.TraceLogger = traceLogger

		resource := NewObject("document", "doc1")
		seq, err := ctx.IterSubjects(fixedIter, resource)
		require.NoError(t, err)

		paths, err := CollectAll(seq)
		require.NoError(t, err)
		require.Len(t, paths, 1)

		// Verify tracing output
		trace := traceLogger.DumpTrace()
		require.True(t, strings.Contains(trace, "-> Fixed: check(document:doc1, :)"))
		require.True(t, strings.Contains(trace, "<- Fixed: returned 1 paths"))
		require.True(t, strings.Contains(trace, "Fixed: iterating subjects for resource document:doc1 from 2 paths"))
		require.True(t, strings.Contains(trace, "Fixed: found 1 matching subjects"))
	})

	t.Run("Union tracing", func(t *testing.T) {
		// Reset trace logger
		traceLogger = NewTraceLogger()
		ctx.TraceLogger = traceLogger

		// Test Union iterator tracing
		union := NewUnion()
		union.addSubIterator(NewFixedIterator(testPath1))
		union.addSubIterator(NewFixedIterator(testPath2))

		seq, err := ctx.Check(union, resources, subject)
		require.NoError(t, err)

		paths, err := CollectAll(seq)
		require.NoError(t, err)
		require.Len(t, paths, 1) // Should match only doc1 for alice

		// Verify tracing output
		trace := traceLogger.DumpTrace()
		require.True(t, strings.Contains(trace, "-> Union: check(document:doc1, user:alice)"))
		require.True(t, strings.Contains(trace, "<- Union: returned 1 paths"))
		require.True(t, strings.Contains(trace, "Union: processing 2 sub-iterators"))
	})
}
