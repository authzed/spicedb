package injector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInjector(t *testing.T) {
	ctx := ContextWithValue[bool](context.Background(), "exists", true)
	ctx = ContextWithValue[bool](ctx, "exists-too", true)
	require.True(t, FromContext[bool](ctx, "exists"))
	require.True(t, FromContext[bool](ctx, "exists-too"))
	require.False(t, FromContext[bool](ctx, "does-not-exist"))
}

type handler[T any] struct {
	t     *testing.T
	key   string
	value T
}

func (h handler[T]) handle(ctx context.Context, req interface{}) (interface{}, error) {
	value := FromContext[T](ctx, h.key)
	require.Equal(h.t, h.value, value)
	return nil, nil
}

func TestUnaryServerInterceptor(t *testing.T) {
	interceptor := UnaryServerInterceptor("test", 1234)
	h := handler[int]{t: t, key: "test", value: 1234}
	_, err := interceptor(context.Background(), 123, nil, h.handle)
	require.NoError(t, err)
}
