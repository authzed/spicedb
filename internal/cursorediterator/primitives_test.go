package cursorediterator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/authzed/spicedb/internal/telemetry/otelconv"
)

func TestJoin(t *testing.T) {
	t.Run("empty iterators", func(t *testing.T) {
		result := join[string]()

		items, errs := collectAll(result)

		require.Len(t, items, 0)
		require.Len(t, errs, 0)
	})

	t.Run("single iterator", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		result := join(iter1)

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b"}, items)
	})

	t.Run("multiple iterators", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("c", nil) {
				return
			}
			if !yield("d", nil) {
				return
			}
		}

		iter3 := func(yield func(string, error) bool) {
			if !yield("e", nil) {
				return
			}
		}

		result := join(iter1, iter2, iter3)

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b", "c", "d", "e"}, items)
	})

	t.Run("iterator with error stops processing", func(t *testing.T) {
		testError := errors.New("test error")

		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("c", nil) {
				return
			}
			if !yield("", testError) { // Error here
				return
			}
			if !yield("d", nil) { // Should not be reached
				return
			}
		}

		iter3 := func(yield func(string, error) bool) {
			if !yield("e", nil) { // Should not be reached
				return
			}
		}

		result := join(iter1, iter2, iter3)

		items, lastError := collectUntilError(result)

		require.Equal(t, []string{"a", "b", "c"}, items)
		require.Equal(t, testError, lastError)
	})

	t.Run("early termination by consumer", func(t *testing.T) {
		iter1 := func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if !yield(2, nil) {
				return
			}
			if !yield(3, nil) { // Should not be reached due to early termination
				return
			}
		}

		iter2 := func(yield func(int, error) bool) {
			if !yield(4, nil) { // Should not be reached
				return
			}
		}

		result := join(iter1, iter2)

		items := collectFirst(t, result, 2)

		require.Equal(t, []int{1, 2}, items)
	})

	t.Run("empty iterator in sequence", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			// Empty iterator - yields nothing
		}

		iter3 := func(yield func(string, error) bool) {
			if !yield("b", nil) {
				return
			}
		}

		result := join(iter1, iter2, iter3)

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b"}, items)
	})

	t.Run("error in first iterator", func(t *testing.T) {
		testError := errors.New("first iterator error")

		iter1 := func(yield func(string, error) bool) {
			if !yield("", testError) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("should not reach", nil) {
				return
			}
		}

		result := join(iter1, iter2)

		items, lastError := collectUntilError(result)

		require.Len(t, items, 0)
		require.Equal(t, testError, lastError)
	})

	t.Run("consumer returns false immediately", func(t *testing.T) {
		iter1 := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return // Consumer returned false, stop yielding
			}
			if !yield("should not reach", nil) {
				return
			}
		}

		iter2 := func(yield func(string, error) bool) {
			if !yield("should not reach", nil) {
				return
			}
		}

		result := join(iter1, iter2)

		// Consumer that immediately returns false
		result(func(item string, err error) bool {
			require.Equal(t, "a", item)
			require.NoError(t, err)
			return false
		})
	})

	t.Run("different types", func(t *testing.T) {
		iter1 := func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if !yield(2, nil) {
				return
			}
		}

		iter2 := func(yield func(int, error) bool) {
			if !yield(3, nil) {
				return
			}
			if !yield(4, nil) {
				return
			}
		}

		result := join(iter1, iter2)

		items := collectNoError(t, result)

		require.Equal(t, []int{1, 2, 3, 4}, items)
	})
}

func TestYieldsError(t *testing.T) {
	t.Run("yields error with default value", func(t *testing.T) {
		testError := errors.New("test error")
		result := YieldsError[string](testError)

		items, errs := collectAll(result)

		require.Len(t, items, 1)
		require.Equal(t, "", items[0]) // Default value for string
		require.Len(t, errs, 1)
		require.Equal(t, testError, errs[0])
	})

	t.Run("yields error with int type", func(t *testing.T) {
		testError := errors.New("integer error")
		result := YieldsError[int](testError)

		items, errs := collectAll(result)

		require.Len(t, items, 1)
		require.Equal(t, 0, items[0]) // Default value for int
		require.Len(t, errs, 1)
		require.Equal(t, testError, errs[0])
	})

	t.Run("consumer returns false immediately", func(t *testing.T) {
		testError := errors.New("test error")
		result := YieldsError[string](testError)

		called := false
		result(func(item string, err error) bool {
			require.False(t, called, "yield should only be called once")
			called = true
			require.Equal(t, "", item)
			require.Equal(t, testError, err)
			return false
		})

		require.True(t, called, "yield should have been called")
	})
}

func TestCountingIterator(t *testing.T) {
	t.Run("counts items with no errors", func(t *testing.T) {
		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
			if !yield("c", nil) {
				return
			}
		}

		var finalCount int
		result := CountingIterator(source, func(count int) {
			finalCount = count
		})

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b", "c"}, items)
		require.Equal(t, 3, finalCount)
	})

	t.Run("does not count items with errors", func(t *testing.T) {
		testError := errors.New("test error")
		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", testError) {
				return
			}
			if !yield("c", nil) {
				return
			}
		}

		var finalCount int
		result := CountingIterator(source, func(count int) {
			finalCount = count
		})

		items, errs := collectAll(result)

		require.Equal(t, []string{"a", "b", "c"}, items)
		require.Len(t, errs, 3)
		require.NoError(t, errs[0])
		require.Equal(t, testError, errs[1])
		require.NoError(t, errs[2])
		require.Equal(t, 2, finalCount) // Only counts items without errors
	})

	t.Run("callback receives correct count", func(t *testing.T) {
		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		var finalCount int
		result := CountingIterator(source, func(count int) {
			finalCount = count
		})

		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b"}, items)
		require.Equal(t, 2, finalCount)
	})

	t.Run("empty iterator results in zero count", func(t *testing.T) {
		source := func(yield func(string, error) bool) {
			// Empty iterator
		}

		var finalCount int
		result := CountingIterator(source, func(count int) {
			finalCount = count
		})

		items := collectNoError(t, result)

		require.Len(t, items, 0)
		require.Equal(t, 0, finalCount)
	})

	t.Run("early termination by consumer", func(t *testing.T) {
		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) { // Should not be reached
				return
			}
			if !yield("c", nil) { // Should not be reached
				return
			}
		}

		var finalCount int
		result := CountingIterator(source, func(count int) {
			finalCount = count
		})

		items := collectFirst(t, result, 1)

		require.Equal(t, []string{"a"}, items)
		require.Equal(t, 1, finalCount) // Only counts what was actually yielded before termination
	})

	t.Run("different types", func(t *testing.T) {
		source := func(yield func(int, error) bool) {
			if !yield(10, nil) {
				return
			}
			if !yield(20, nil) {
				return
			}
			if !yield(30, nil) {
				return
			}
		}

		var finalCount int
		result := CountingIterator(source, func(count int) {
			finalCount = count
		})

		items := collectNoError(t, result)

		require.Equal(t, []int{10, 20, 30}, items)
		require.Equal(t, 3, finalCount)
	})

	t.Run("callback is invoked after iteration completes", func(t *testing.T) {
		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		callbackInvoked := false
		var finalCount int
		result := CountingIterator(source, func(count int) {
			callbackInvoked = true
			finalCount = count
		})

		// Before iteration, callback should not be invoked
		require.False(t, callbackInvoked)
		require.Equal(t, 0, finalCount)

		items := collectNoError(t, result)

		// After iteration, callback should be invoked with correct count
		require.True(t, callbackInvoked)
		require.Equal(t, []string{"a", "b"}, items)
		require.Equal(t, 2, finalCount)
	})
}

func TestUncursoredEmpty(t *testing.T) {
	t.Run("yields no items", func(t *testing.T) {
		result := UncursoredEmpty[string]()

		items, errs := collectAll(result)

		require.Len(t, items, 0)
		require.Len(t, errs, 0)
	})

	t.Run("works with different types", func(t *testing.T) {
		result := UncursoredEmpty[int]()

		items, errs := collectAll(result)

		require.Len(t, items, 0)
		require.Len(t, errs, 0)
	})

	t.Run("consumer function never called", func(t *testing.T) {
		result := UncursoredEmpty[string]()

		called := false
		result(func(item string, err error) bool {
			called = true
			return true
		})

		require.False(t, called, "yield function should never be called for empty iterator")
	})
}

func TestSpanned(t *testing.T) {
	setupTracer := func() (*tracetest.SpanRecorder, func()) {
		spanRecorder := tracetest.NewSpanRecorder()
		tracerProvider := trace.NewTracerProvider(
			trace.WithSampler(trace.AlwaysSample()),
			trace.WithSpanProcessor(spanRecorder),
		)

		// Save and restore the original tracer provider
		originalProvider := otel.GetTracerProvider()
		otel.SetTracerProvider(tracerProvider)

		return spanRecorder, func() {
			otel.SetTracerProvider(originalProvider)
		}
	}

	t.Run("creates span with correct name and attributes", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		spanName := "test-span"
		spanAttrs := []attribute.KeyValue{
			attribute.String(otelconv.AttrTestKey, "test.value"),
			attribute.Int(otelconv.AttrTestNumber, 42),
		}

		result := Spanned(ctx, source, tracer, spanName, spanAttrs...)
		items := collectNoError(t, result)

		require.Equal(t, []string{"a", "b"}, items)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		require.Equal(t, spanName, span.Name())

		// Check that the specified attributes are present
		attrs := span.Attributes()
		foundTestKey := false
		foundTestNumber := false
		foundItemCount := false

		for _, attr := range attrs {
			switch attr.Key {
			case otelconv.AttrTestKey:
				require.Equal(t, "test.value", attr.Value.AsString())
				foundTestKey = true
			case otelconv.AttrTestNumber:
				require.Equal(t, int64(42), attr.Value.AsInt64())
				foundTestNumber = true
			case otelconv.AttrIteratorItemCount:
				require.Equal(t, int64(2), attr.Value.AsInt64())
				foundItemCount = true
			}
		}

		require.True(t, foundTestKey, "test.key attribute not found")
		require.True(t, foundTestNumber, "test.number attribute not found")
		require.True(t, foundItemCount, "item.count attribute not found")
	})

	t.Run("records errors to span", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		testError1 := errors.New("first error")
		testError2 := errors.New("second error")

		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", testError1) {
				return
			}
			if !yield("c", nil) {
				return
			}
			if !yield("d", testError2) {
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "test-span")

		items, errs := collectAll(result)

		require.Equal(t, []string{"a", "b", "c", "d"}, items)
		require.Len(t, errs, 4)
		require.NoError(t, errs[0])
		require.Equal(t, testError1, errs[1])
		require.NoError(t, errs[2])
		require.Equal(t, testError2, errs[3])

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]

		// Check that errors were recorded
		events := span.Events()
		errorEvents := 0
		for _, event := range events {
			if event.Name == "exception" {
				errorEvents++
			}
		}
		require.Equal(t, 2, errorEvents, "Expected 2 error events to be recorded")
	})

	t.Run("sets correct item count attribute", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			for i := 0; i < 5; i++ {
				if !yield("item", nil) {
					return
				}
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "test-span")

		items := collectNoError(t, result)

		require.Len(t, items, 5)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		attrs := span.Attributes()

		foundItemCount := false
		for _, attr := range attrs {
			if attr.Key == otelconv.AttrIteratorItemCount {
				require.Equal(t, int64(5), attr.Value.AsInt64())
				foundItemCount = true
				break
			}
		}

		require.True(t, foundItemCount, "item.count attribute not found")
	})

	t.Run("counts items correctly even with errors", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		testError := errors.New("test error")
		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", testError) {
				return
			}
			if !yield("c", nil) {
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "test-span")

		items, _ := collectAll(result)

		require.Len(t, items, 3)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		attrs := span.Attributes()

		foundItemCount := false
		for _, attr := range attrs {
			if attr.Key == otelconv.AttrIteratorItemCount {
				require.Equal(t, int64(3), attr.Value.AsInt64())
				foundItemCount = true
				break
			}
		}

		require.True(t, foundItemCount, "item.count attribute not found")
	})

	t.Run("ends span when iteration completes normally", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "test-span")

		_ = collectNoError(t, result)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		require.True(t, span.EndTime().After(span.StartTime()))
	})

	t.Run("ends span when iteration terminates early", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			if !yield("a", nil) {
				return
			}
			if !yield("b", nil) {
				return
			}
			if !yield("c", nil) { // Should not be reached
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "test-span")

		items := collectFirst(t, result, 2)

		require.Equal(t, []string{"a", "b"}, items)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		require.True(t, span.EndTime().After(span.StartTime()))

		// Check item count is correct for early termination
		attrs := span.Attributes()
		foundItemCount := false
		for _, attr := range attrs {
			if attr.Key == otelconv.AttrIteratorItemCount {
				require.Equal(t, int64(2), attr.Value.AsInt64())
				foundItemCount = true
				break
			}
		}
		require.True(t, foundItemCount, "item.count attribute not found")
	})

	t.Run("handles empty iterator", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			// Empty iterator
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "test-span")

		items := collectNoError(t, result)

		require.Len(t, items, 0)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		attrs := span.Attributes()

		foundItemCount := false
		for _, attr := range attrs {
			if attr.Key == otelconv.AttrIteratorItemCount {
				require.Equal(t, int64(0), attr.Value.AsInt64())
				foundItemCount = true
				break
			}
		}

		require.True(t, foundItemCount, "item.count attribute not found")
	})

	t.Run("works with different item types", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(int, error) bool) {
			if !yield(10, nil) {
				return
			}
			if !yield(20, nil) {
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "int-span")

		items := collectNoError(t, result)

		require.Equal(t, []int{10, 20}, items)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		require.Equal(t, "int-span", span.Name())
	})

	t.Run("preserves span context", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			if !yield("test", nil) {
				return
			}
		}

		// Create a parent span context
		tracer := otel.Tracer("test-tracer")
		parentCtx, parentSpan := tracer.Start(context.Background(), "parent-span")
		defer parentSpan.End()

		result := Spanned(parentCtx, source, tracer, "child-span")
		_ = collectNoError(t, result)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		childSpan := spans[0]
		require.Equal(t, "child-span", childSpan.Name())

		// The child span should have the parent span as its parent
		require.True(t, childSpan.Parent().IsValid())
		require.Equal(t, parentSpan.SpanContext().SpanID(), childSpan.Parent().SpanID())
	})

	t.Run("no attributes provided", func(t *testing.T) {
		spanRecorder, cleanup := setupTracer()
		defer cleanup()

		source := func(yield func(string, error) bool) {
			if !yield("test", nil) {
				return
			}
		}

		ctx := context.Background()
		tracer := otel.Tracer("test-tracer")
		result := Spanned(ctx, source, tracer, "no-attrs-span")

		_ = collectNoError(t, result)

		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)

		span := spans[0]
		require.Equal(t, "no-attrs-span", span.Name())

		// Should still have the item.count attribute
		attrs := span.Attributes()
		foundItemCount := false
		for _, attr := range attrs {
			if attr.Key == otelconv.AttrIteratorItemCount {
				require.Equal(t, int64(1), attr.Value.AsInt64())
				foundItemCount = true
				break
			}
		}

		require.True(t, foundItemCount, "item.count attribute not found")
	})
}
