package cursorediterator

import (
	"context"
	"iter"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/telemetry/otelconv"
)

// join combines multiple iterator sequences into one.
func join[I any](iters ...iter.Seq2[I, error]) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		for _, it := range iters {
			canceled := false
			y := func(i I, err error) bool {
				if canceled {
					return false
				}
				if !yield(i, err) {
					canceled = true
					return false
				}
				if err != nil {
					canceled = true
					return false
				}
				return true
			}
			it(y)
			if canceled {
				break
			}
		}
	}
}

// YieldsError creates an iterator that yields a default value and an error.
func YieldsError[I any](err error) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		var defaultValue I
		if !yield(defaultValue, err) {
			return
		}
	}
}

// UncursoredEmpty is a function that returns an empty iterator sequence without a cursor.
func UncursoredEmpty[I any]() iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {}
}

// CountingIterator wraps a Seq2 iterator and counts the number of items yielded,
// invoking the callback with the final count once the iterator completes.
func CountingIterator[I any](source iter.Seq2[I, error], callback func(int)) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		count := 0
		source(func(item I, err error) bool {
			if err == nil {
				count++
			}
			return yield(item, err)
		})
		callback(count)
	}
}

// Spanned wraps an iterator sequence, recording any errors to the provided span,
// and ending the span when the iteration is complete.
func Spanned[I any](
	ctx context.Context,
	source iter.Seq2[I, error],
	tracer trace.Tracer,
	spanName string,
	spanAttrs ...attribute.KeyValue,
) iter.Seq2[I, error] {
	return func(yield func(I, error) bool) {
		_, span := tracer.Start(ctx, spanName, trace.WithAttributes(spanAttrs...))
		defer span.End()

		itemCount := 0
		for item, err := range source {
			if err != nil {
				span.RecordError(err)
			}

			itemCount++
			if !yield(item, err) {
				break
			}
		}

		span.SetAttributes(attribute.Int(otelconv.AttrIteratorItemCount, itemCount))
	}
}
