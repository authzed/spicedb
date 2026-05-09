package slogcommon

import (
	"context"
	"log/slog"
)

func ContextExtractor(ctx context.Context, fns []func(ctx context.Context) []slog.Attr) []slog.Attr {
	attrs := []slog.Attr{}
	for _, fn := range fns {
		attrs = append(attrs, fn(ctx)...)
	}
	return attrs
}

func ExtractFromContext(keys ...any) func(ctx context.Context) []slog.Attr {
	return func(ctx context.Context) []slog.Attr {
		attrs := make([]slog.Attr, 0, len(keys))
		for _, key := range keys {
			attrs = append(attrs, slog.Any(key.(string), ctx.Value(key)))
		}
		return attrs
	}
}
