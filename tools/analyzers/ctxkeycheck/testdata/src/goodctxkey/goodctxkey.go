package goodctxkey

import (
	"context"
	"time"

	"thirdparty"
)

type contextKey struct{}

type fakeContext struct{}

// WithValue is a method on a local type, not context.WithValue.
func (fakeContext) WithValue(key, value any) fakeContext { return fakeContext{} }

// WithValue is a local function, not context.WithValue.
func WithValue(ctx context.Context, key, value any) context.Context { return ctx }

func ReadValue(ctx context.Context) any {
	// Reading a value is allowed: keys set by third-party libraries can only be read this way.
	return ctx.Value(contextKey{})
}

func OtherContextFuncs(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return context.WithTimeout(ctx, time.Second)
}

func LocalWithValue(ctx context.Context) context.Context {
	_ = fakeContext{}.WithValue(contextKey{}, 42)
	return WithValue(ctx, contextKey{}, 42)
}

func ShadowedPackage(ctx context.Context) {
	context := fakeContext{}
	_ = context.WithValue(contextKey{}, 42)
}

func AllowedForeignKey(ctx context.Context) context.Context {
	// The key's type is listed in allowed-key-types.
	return context.WithValue(ctx, thirdparty.CtxKey, []byte("key"))
}
