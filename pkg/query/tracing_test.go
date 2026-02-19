package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
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

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)),
		WithTraceLogger(traceLogger),
	)

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
	require.Contains(t, trace, "-> ")
	require.Contains(t, trace, "Fixed")
	require.Contains(t, trace, "check(document:doc1, user:alice)")
	require.Contains(t, trace, "<- ")
	require.Contains(t, trace, "returned 1 paths")
	require.Contains(t, trace, "checking 2 paths against 1 resources")
	require.Contains(t, trace, "found 1 matching paths")

	t.Run("IterSubjects tracing", func(t *testing.T) {
		// Reset trace logger
		traceLogger = NewTraceLogger()
		ctx.TraceLogger = traceLogger

		resource := NewObject("document", "doc1")
		seq, err := ctx.IterSubjects(fixedIter, resource, NoObjectFilter())
		require.NoError(t, err)

		paths, err := CollectAll(seq)
		require.NoError(t, err)
		require.Len(t, paths, 1)

		// Verify tracing output
		trace := traceLogger.DumpTrace()
		require.Contains(t, trace, "-> ")
		require.Contains(t, trace, "Fixed")
		require.Contains(t, trace, "iterSubjects(document:doc1)")
		require.Contains(t, trace, "<- ")
		require.Contains(t, trace, "returned 1 paths")
		require.Contains(t, trace, "iterating subjects for resource document:doc1 from 2 paths")
		require.Contains(t, trace, "found 1 matching subjects")
	})

	t.Run("Union tracing", func(t *testing.T) {
		// Reset trace logger
		traceLogger = NewTraceLogger()
		ctx.TraceLogger = traceLogger

		// Test Union iterator tracing
		union := NewUnionIterator(NewFixedIterator(testPath1), NewFixedIterator(testPath2))

		seq, err := ctx.Check(union, resources, subject)
		require.NoError(t, err)

		paths, err := CollectAll(seq)
		require.NoError(t, err)
		require.Len(t, paths, 1) // Should match only doc1 for alice

		// Verify tracing output
		trace := traceLogger.DumpTrace()
		require.Contains(t, trace, "-> ")
		require.Contains(t, trace, "Union")
		require.Contains(t, trace, "check(document:doc1, user:alice)")
		require.Contains(t, trace, "<- ")
		require.Contains(t, trace, "returned 1 paths")
		require.Contains(t, trace, "processing 2 sub-iterators")
	})
}
