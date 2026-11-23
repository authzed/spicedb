package badctxkey

import (
	"context"
	stdctx "context"

	"thirdparty"
)

type contextKey struct{}

type stringKey string

const someKey stringKey = "some"

func StructKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey{}, 42) // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
}

func StringKey(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, someKey, "value") // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
	return ctx
}

func RenamedImport(ctx stdctx.Context) stdctx.Context {
	return stdctx.WithValue(ctx, contextKey{}, 42) // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
}

func Nested(ctx context.Context) context.Context {
	return context.WithValue( // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
		context.WithValue(ctx, contextKey{}, 1), // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
		someKey, "value",
	)
}

func InClosure(ctx context.Context) func() context.Context {
	return func() context.Context {
		return context.WithValue(ctx, contextKey{}, 42) // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
	}
}

func ForeignKeyNotAllowed(ctx context.Context) context.Context {
	return context.WithValue(ctx, thirdparty.OtherKey, 42) // want "use a typed key from github.com/authzed/ctxkey instead of context.WithValue"
}
