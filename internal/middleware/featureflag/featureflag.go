package featureflag

import (
	"context"

	"github.com/authzed/spicedb/internal/middleware/injector"

	"google.golang.org/grpc"
)

var key = struct{}{}

type Flags map[string]struct{}

func WithFlag(ctx context.Context, flag string) context.Context {
	flags := injector.FromContext[Flags](ctx, key)
	if flags == nil {
		flags = map[string]struct{}{flag: {}}
		return injector.ContextWithValue(ctx, key, flags)
	}
	flags[flag] = struct{}{}
	return ctx
}

func FromContext(ctx context.Context, flag string) bool {
	flags := injector.FromContext[Flags](ctx, key)
	_, ok := flags[flag]
	return ok
}

func UnaryServerInterceptor(flags []string) grpc.UnaryServerInterceptor {
	return injector.UnaryServerInterceptor(key, FromStringSlice(flags))
}

func StreamServerInterceptor(flags []string) grpc.StreamServerInterceptor {
	return injector.StreamServerInterceptor(key, FromStringSlice(flags))
}

func FromStringSlice(flags []string) Flags {
	flagMap := make(map[string]struct{}, len(flags))
	for _, flag := range flags {
		flagMap[flag] = struct{}{}
	}
	return flagMap
}
