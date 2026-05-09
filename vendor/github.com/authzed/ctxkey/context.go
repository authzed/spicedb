// Package ctxkey provides generic helpers for storing / retrieving values
// from a `context.Context`.
package ctxkey

import (
	"context"
	"fmt"
)

// ContextWith is an interface for producting functions that can add a value to
// a context.Context.
type ContextWith[V any] interface {
	With(val V) func(ctx context.Context) context.Context
}

// ContextSet is an interface for types that can be used to add a value directly
// to a context.Context.
type ContextSet[V any] interface {
	Set(ctx context.Context, val V) context.Context
}

// Key is a type that is used as a key in a context.Context for a
// specific type of value V.
type Key[V any] struct{}

// New creates a new Key
func New[V any]() *Key[V] {
	return &Key[V]{}
}

// Set adds a value to the context for this key.
func (k *Key[V]) Set(ctx context.Context, val V) context.Context {
	return context.WithValue(ctx, k, val)
}

// With returns a fn that adds a value to the context for this key.
func (k *Key[V]) With(val V) func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		return k.Set(ctx, val)
	}
}

// Value retrieves the value from the context for this key. It returns the value
// and a boolean indicating if the value was found.
func (k *Key[V]) Value(ctx context.Context) (V, bool) {
	v, ok := ctx.Value(k).(V)
	return v, ok
}

// MustValue retrieves the value from the context for this key. It panics if the
// value is not found.
func (k *Key[V]) MustValue(ctx context.Context) V {
	v, ok := k.Value(ctx)
	if !ok {
		panic(fmt.Sprintf("could not find value for key %T in context", k))
	}
	return v
}

// DefaultingKey is a type that is used as a key in a context.Context for
// a specific type of value, but returns the default value for V if unset.
type DefaultingKey[V comparable] struct {
	defaultValue V
}

// NewWithDefault creates a new DefaultingKey with the given default value
func NewWithDefault[V comparable](defaultValue V) *DefaultingKey[V] {
	return &DefaultingKey[V]{
		defaultValue: defaultValue,
	}
}

// Set adds a value to the context for this key.
func (k *DefaultingKey[V]) Set(ctx context.Context, val V) context.Context {
	return context.WithValue(ctx, k, val)
}

// With returns a fn that adds a value to the context for this key.
func (k *DefaultingKey[V]) With(val V) func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		return k.Set(ctx, val)
	}
}

// Value retrieves the value from the context for this key. If the value is not
// found, it returns the default value.
func (k *DefaultingKey[V]) Value(ctx context.Context) V {
	v, ok := ctx.Value(k).(V)
	if !ok {
		return k.defaultValue
	}
	return v
}

// MustNonEmptyValue retrieves the value from the context for this key. If the value is
// empty, it panics.
func (k *DefaultingKey[V]) MustNonEmptyValue(ctx context.Context) V {
	v := k.Value(ctx)
	var empty V
	if v == empty {
		panic(fmt.Sprintf("could not find non-nil value for key %T in context", k))
	}
	return v
}

type Box[V any] struct {
	value V
}

// BoxedKey is a type that is used as a key in a
// context.Context that points to a handle containing the desired value.
// This allows a handler higher up in the chain to carve out a spot to be
// filled in by other handlers.
// It can also be used to hold non-comparable objects by wrapping them with a
// pointer.
type BoxedKey[V any] struct {
	defaultValue V
}

// NewBoxedWithDefault creates a new BoxedKey with a default value
func NewBoxedWithDefault[V any](defaultValue V) *BoxedKey[V] {
	return &BoxedKey[V]{
		defaultValue: defaultValue,
	}
}

// Set adds a boxed value to the context for this key.
func (k *BoxedKey[V]) Set(ctx context.Context, val V) context.Context {
	handle, ok := ctx.Value(k).(*Box[V])
	if ok {
		handle.value = val
		return ctx
	}
	return context.WithValue(ctx, k, &Box[V]{value: val})
}

// With returns a fn that adds boxed value to the context for this key.
func (k *BoxedKey[V]) With(val V) func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		return k.Set(ctx, val)
	}
}

// SetBox adds a default boxed value to the context for this key.
func (k *BoxedKey[V]) SetBox(ctx context.Context) context.Context {
	return context.WithValue(ctx, k, &Box[V]{value: k.defaultValue})
}

// WithBox adds a box with the default value to the context for this key.
func (k *BoxedKey[V]) WithBox() func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		return k.SetBox(ctx)
	}
}

// Value retrieves the value from the context for this key. If the value is not
// found, it returns the default value.
func (k *BoxedKey[V]) Value(ctx context.Context) V {
	handle, ok := ctx.Value(k).(*Box[V])
	if !ok {
		return k.defaultValue
	}

	return handle.value
}

// With takes a list of functions that modify a context and returns a new
// function that applies all of them. This can be used with the `.With(value)`
// methods on keys to apply multiple values to a context.
func With(values ...func(context.Context) context.Context) func(ctx context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		for _, f := range values {
			ctx = f(ctx)
		}
		return ctx
	}
}
