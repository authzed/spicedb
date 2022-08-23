package featureflag

import (
	"context"

	"github.com/authzed/spicedb/internal/middleware/injector"

	"google.golang.org/grpc"
)

var key = struct{}{}

type Flags map[string]bool

func WithFlag(ctx context.Context, flag string, enabled bool) context.Context {
	flags := injector.FromContext[Flags](ctx, key)
	if flags == nil {
		flags = map[string]bool{flag: enabled}
		return injector.ContextWithValue(ctx, key, flags)
	}
	flags[flag] = enabled
	return ctx
}

func FromContext(ctx context.Context, flag string) bool {
	flags := injector.FromContext[Flags](ctx, key)
	return flags[flag]
}

func UnaryServerInterceptor(value Flags) grpc.UnaryServerInterceptor {
	return injector.UnaryServerInterceptor(key, value)
}

func StreamServerInterceptor(flags Flags) grpc.StreamServerInterceptor {
	return injector.StreamServerInterceptor(key, flags)
}
