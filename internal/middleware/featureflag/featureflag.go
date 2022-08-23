package featureflag

import (
	"context"

	"github.com/authzed/spicedb/internal/middleware/injector"

	"google.golang.org/grpc"
)

var key = struct{}{}

type flags map[string]struct{}

// WithFlag injects in the context the name of an enabled feature flag
func WithFlag(ctx context.Context, flag string) context.Context {
	f := injector.FromContext[flags](ctx, key)
	if f == nil {
		f = map[string]struct{}{flag: {}}
		return injector.ContextWithValue(ctx, key, f)
	}
	f[flag] = struct{}{}
	return ctx
}

// FromContext returns a value of a feature flag specified by its name.
func FromContext(ctx context.Context, flag string) bool {
	f := injector.FromContext[flags](ctx, key)
	_, ok := f[flag]
	return ok
}

// UnaryServerInterceptor returns a new unary server interceptor that injects
// the state of the applications feature flags in the context
func UnaryServerInterceptor(flags []string) grpc.UnaryServerInterceptor {
	return injector.UnaryServerInterceptor(key, fromStringSlice(flags))
}

// StreamServerInterceptor returns a new stream server interceptor that injects
// the state of the applications feature flags in the context
func StreamServerInterceptor(flags []string) grpc.StreamServerInterceptor {
	return injector.StreamServerInterceptor(key, fromStringSlice(flags))
}

func fromStringSlice(flags []string) flags {
	flagMap := make(map[string]struct{}, len(flags))
	for _, flag := range flags {
		flagMap[flag] = struct{}{}
	}
	return flagMap
}
