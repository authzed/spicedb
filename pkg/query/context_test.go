package query

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestTraceLogger(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("NewTraceLogger", func(t *testing.T) {
		logger := NewTraceLogger()
		require.NotNil(logger)
		require.Empty(logger.traces)
		require.Equal(0, logger.depth)
		require.Empty(logger.stack)
	})

	t.Run("EnterIteratorOld", func(t *testing.T) {
		logger := NewTraceLogger()
		resources := []Object{NewObject("document", "doc1"), NewObject("document", "doc2")}
		subject := NewObject("user", "alice").WithEllipses()

		logger.EnterIteratorOld("TestIterator", resources, subject)

		require.Len(logger.traces, 1)
		require.Equal(1, logger.depth)
		require.Contains(logger.traces[0], "-> TestIterator: check(document:doc1,document:doc2, user:alice)")
	})

	t.Run("EnterIterator", func(t *testing.T) {
		logger := NewTraceLogger()
		resources := []Object{NewObject("document", "doc1")}
		subject := NewObject("user", "alice").WithEllipses()

		// Create a test iterator with known Explain output
		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		logger.EnterIterator(iterator, resources, subject)

		require.Len(logger.traces, 1)
		require.Equal(1, logger.depth)
		require.Len(logger.stack, 1)
		require.Contains(logger.traces[0], "-> Fixed: check(document:doc1, user:alice)")
	})

	t.Run("ExitIteratorOld", func(t *testing.T) {
		logger := NewTraceLogger()
		logger.depth = 1 // Simulate having entered an iterator

		paths := []*Path{MustPathFromString("document:doc1#view@user:alice")}
		logger.ExitIteratorOld("TestIterator", paths)

		require.Equal(0, logger.depth)
		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "<- TestIterator: returned 1 paths")
		require.Contains(logger.traces[0], "document:doc1#view@user:alice")
	})

	t.Run("ExitIterator", func(t *testing.T) {
		logger := NewTraceLogger()
		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		// Simulate entering the iterator first
		logger.stack = append(logger.stack, iterator)
		logger.depth = 1

		paths := []*Path{testPath}
		logger.ExitIterator(iterator, paths)

		require.Equal(0, logger.depth)
		require.Empty(logger.stack)
		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "<- Fixed: returned 1 paths")
	})

	t.Run("LogStepOld", func(t *testing.T) {
		logger := NewTraceLogger()
		logger.depth = 2 // Simulate being nested

		logger.LogStepOld("TestIterator", "processing step %d", 42)

		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "    TestIterator: processing step 42")
	})

	t.Run("LogStep", func(t *testing.T) {
		logger := NewTraceLogger()
		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		// Add iterator to stack
		logger.stack = append(logger.stack, iterator)

		logger.LogStep(iterator, "processing data: %s", "test_data")

		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "Fixed: processing data: test_data")
	})

	t.Run("LogStep_IteratorNotInStack", func(t *testing.T) {
		logger := NewTraceLogger()
		logger.depth = 3
		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		// Don't add iterator to stack - should use current depth
		logger.LogStep(iterator, "fallback message")

		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "      Fixed: fallback message") // 3 levels of indentation
	})

	t.Run("DumpTrace", func(t *testing.T) {
		logger := NewTraceLogger()
		logger.traces = []string{"line1", "line2", "line3"}

		dump := logger.DumpTrace()
		require.Equal("line1\nline2\nline3", dump)
	})

	t.Run("Caveat_handling_in_traces", func(t *testing.T) {
		logger := NewTraceLogger()
		logger.depth = 1 // Simulate having entered an iterator

		// Create path with caveat
		pathWithCaveat := MustPathFromString("document:doc1#view@user:alice")
		pathWithCaveat.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{CaveatName: "test_caveat"},
			},
		}

		// Test ExitIteratorOld with caveat
		logger.ExitIteratorOld("CaveatIterator", []*Path{pathWithCaveat})

		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "[test_caveat]")
	})

	t.Run("Complex_caveat_handling", func(t *testing.T) {
		logger := NewTraceLogger()
		logger.depth = 1 // Simulate having entered an iterator

		// Create path with complex caveat (operation-based)
		pathWithComplexCaveat := MustPathFromString("document:doc1#view@user:alice")
		pathWithComplexCaveat.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Operation{
				Operation: &core.CaveatOperation{Op: core.CaveatOperation_AND},
			},
		}

		logger.ExitIteratorOld("ComplexIterator", []*Path{pathWithComplexCaveat})

		require.Len(logger.traces, 1)
		require.Contains(logger.traces[0], "[complex_caveat]")
	})
}

func TestContext(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("TraceStep", func(t *testing.T) {
		logger := NewTraceLogger()
		ctx := &Context{
			Context:     context.Background(),
			TraceLogger: logger,
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		ctx.TraceStep(iterator, "test message: %s", "data")

		traces := logger.DumpTrace()
		require.Contains(traces, "Fixed: test message: data")
	})

	t.Run("TraceStep_NoLogger", func(t *testing.T) {
		ctx := &Context{
			Context: context.Background(),
			// No TraceLogger set
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		// Should not panic when no logger is set
		require.NotPanics(func() {
			ctx.TraceStep(iterator, "test message")
		})
	})

	t.Run("TraceEnter", func(t *testing.T) {
		logger := NewTraceLogger()
		ctx := &Context{
			Context:     context.Background(),
			TraceLogger: logger,
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)
		resources := []Object{NewObject("document", "doc1")}
		subject := NewObject("user", "alice").WithEllipses()

		ctx.TraceEnter(iterator, resources, subject)

		require.Equal(1, logger.depth)
		require.Len(logger.stack, 1)
	})

	t.Run("TraceExit", func(t *testing.T) {
		logger := NewTraceLogger()
		ctx := &Context{
			Context:     context.Background(),
			TraceLogger: logger,
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		// Simulate having entered
		logger.stack = append(logger.stack, iterator)
		logger.depth = 1

		ctx.TraceExit(iterator, []*Path{testPath})

		require.Equal(0, logger.depth)
		require.Empty(logger.stack)
	})

	t.Run("shouldTrace", func(t *testing.T) {
		// With logger
		ctx := &Context{
			Context:     context.Background(),
			TraceLogger: NewTraceLogger(),
		}
		require.True(ctx.shouldTrace())

		// Without logger
		ctx.TraceLogger = nil
		require.False(ctx.shouldTrace())
	})

	t.Run("Check_NoExecutor", func(t *testing.T) {
		ctx := &Context{
			Context: context.Background(),
			// No Executor set
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		require.Panics(func() {
			ctx.Check(iterator, []Object{NewObject("document", "doc1")}, NewObject("user", "alice").WithEllipses())
		})
	})

	t.Run("IterSubjects_NoExecutor", func(t *testing.T) {
		ctx := &Context{
			Context: context.Background(),
			// No Executor set
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		require.Panics(func() {
			ctx.IterSubjects(iterator, NewObject("document", "doc1"))
		})
	})

	t.Run("IterResources_NoExecutor", func(t *testing.T) {
		ctx := &Context{
			Context: context.Background(),
			// No Executor set
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		require.Panics(func() {
			ctx.IterResources(iterator, NewObject("user", "alice").WithEllipses())
		})
	})

	t.Run("wrapPathSeqForTracing_NoTracing", func(t *testing.T) {
		ctx := &Context{
			Context: context.Background(),
			// No TraceLogger
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		originalSeq := func(yield func(*Path, error) bool) {
			yield(testPath, nil)
		}

		wrappedSeq := ctx.wrapPathSeqForTracing(iterator, originalSeq)

		// Should return the original sequence unchanged when no tracing
		require.NotNil(wrappedSeq)

		// Collect results to verify it works
		var paths []*Path
		for path, err := range wrappedSeq {
			require.NoError(err)
			paths = append(paths, path)
		}
		require.Len(paths, 1)
		require.Equal(testPath, paths[0])
	})

	t.Run("wrapPathSeqForTracing_WithTracing", func(t *testing.T) {
		logger := NewTraceLogger()
		ctx := &Context{
			Context:     context.Background(),
			TraceLogger: logger,
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)

		// Simulate having entered the iterator
		logger.stack = append(logger.stack, iterator)
		logger.depth = 1

		originalSeq := func(yield func(*Path, error) bool) {
			yield(testPath, nil)
		}

		wrappedSeq := ctx.wrapPathSeqForTracing(iterator, originalSeq)

		// Collect results
		var paths []*Path
		for path, err := range wrappedSeq {
			require.NoError(err)
			paths = append(paths, path)
		}

		require.Len(paths, 1)
		require.Equal(testPath, paths[0])

		// Should have generated an exit trace
		traces := logger.DumpTrace()
		require.Contains(traces, "<- Fixed: returned 1 paths")
	})

	t.Run("wrapPathSeqForTracing_WithError", func(t *testing.T) {
		logger := NewTraceLogger()
		ctx := &Context{
			Context:     context.Background(),
			TraceLogger: logger,
		}

		testPath := MustPathFromString("document:doc1#view@user:alice")
		iterator := NewFixedIterator(testPath)
		logger.stack = append(logger.stack, iterator)
		logger.depth = 1 // Set proper depth to avoid negative repeat count

		expectedErr := errors.New("test error")
		originalSeq := func(yield func(*Path, error) bool) {
			yield(nil, expectedErr)
		}

		wrappedSeq := ctx.wrapPathSeqForTracing(iterator, originalSeq)

		// Collect results - should get the error
		var foundError error
		for path, err := range wrappedSeq {
			require.Nil(path)
			foundError = err
			break
		}

		require.Equal(expectedErr, foundError)
	})
}