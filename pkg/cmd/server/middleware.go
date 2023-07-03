package server

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/spiceerrors"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
)

type middlewareTypes interface {
	grpc.UnaryServerInterceptor | grpc.StreamServerInterceptor
}

// MiddlewareChain describes an ordered sequence of middlewares that can be modified
// with one or more MiddlewareModification. This struct is used to facilitate the
// creation and modification of gRPC middleware chains
type MiddlewareChain[T middlewareTypes] struct {
	chain []ReferenceableMiddleware[T]
}

// NewMiddlewareChain creates a new middleware chain given zero or more named middlewares.
// An error will be returned in case validation of the NamedMiddlewares fail.
func NewMiddlewareChain[T middlewareTypes](mw ...ReferenceableMiddleware[T]) (MiddlewareChain[T], error) {
	if err := validate(mw); err != nil {
		return MiddlewareChain[T]{}, err
	}
	return MiddlewareChain[T]{chain: mw}, nil
}

// MiddlewareModification describes an operation to modify a MiddlewareChain
type MiddlewareModification[T middlewareTypes] struct {
	// DependencyMiddlewareName is used to define with respect to which middleware an operation is performed.
	// Dependency is not required for ReplaceAll operation
	DependencyMiddlewareName string

	// Operation describes the type of operation to be performed
	Operation MiddlewareOperation

	// Middlewares are the named middlewares that will be part of this modification
	Middlewares []ReferenceableMiddleware[T]
}

func (mm MiddlewareModification[T]) validate() error {
	if mm.Operation != OperationReplaceAllUnsafe && mm.DependencyMiddlewareName == "" {
		return fmt.Errorf("cannot perform middleware modification without a dependency: %v", mm)
	}
	return validate(mm.Middlewares)
}

func validate[T middlewareTypes](mws []ReferenceableMiddleware[T]) error {
	names := mapz.NewSet[string]()
	for _, mw := range mws {
		if mw.Name == "" {
			return fmt.Errorf("unnamed middleware found: %v", mw)
		}
		if !names.Add(mw.Name) {
			return fmt.Errorf("found middleware with duplicate names in middleware modification: %s", mw.Name)
		}
	}
	return nil
}

// ReferenceableMiddleware represents a middleware in a MiddlewareChain. Middlewares can
// be referenced by name in MiddlewareModification, for example "append after middleware abc".
// Internal middlewares can also be referenced for operations like append or prepend, but cannot
// be referenced for replace operations. Middlewares must always be named.
type ReferenceableMiddleware[T middlewareTypes] struct {
	Name       string
	Internal   bool
	Middleware T
}

// MiddlewareOperation describes the type of operation that will be performed in a MiddlewareModification
type MiddlewareOperation int

const (
	// OperationPrepend adds the middlewares right before the referenced dependency
	OperationPrepend MiddlewareOperation = iota

	// OperationReplace substitutes the referenced dependency with the middlewares of a modification.
	// If replaced with an empty modification, this acts like a deletion
	OperationReplace

	// OperationAppend adds the middlewares right after the referenced dependency
	OperationAppend

	// OperationReplaceAllUnsafe replaces all middlewares in a chain with those in the modification
	// this operation is only meant to be used in tests.
	OperationReplaceAllUnsafe
)

// Names returns the names of the middlewares in a chain
func (mc *MiddlewareChain[T]) Names() *mapz.Set[string] {
	names := mapz.NewSet[string]()
	for _, mw := range mc.chain {
		names.Add(mw.Name)
	}
	return names
}

// ToGRPCInterceptors generates slices of gRPC interceptors ready to be installed in a server
func (mc *MiddlewareChain[T]) ToGRPCInterceptors() []T {
	interceptors := make([]T, 0, len(mc.chain))
	for _, mw := range mc.chain {
		interceptors = append(interceptors, mw.Middleware)
	}
	return interceptors
}

func (mc *MiddlewareChain[T]) prepend(mod MiddlewareModification[T]) error {
	if err := mc.validate(mod); err != nil {
		return err
	}

	newChain := make([]ReferenceableMiddleware[T], 0, len(mc.chain))
	for _, mw := range mc.chain {
		if mw.Name == mod.DependencyMiddlewareName {
			newChain = append(newChain, mod.Middlewares...)
		}
		newChain = append(newChain, mw)
	}
	mc.chain = newChain
	return nil
}

func (mc *MiddlewareChain[T]) replace(mod MiddlewareModification[T]) error {
	if err := mc.validate(mod); err != nil {
		return err
	}
	newChain := make([]ReferenceableMiddleware[T], 0, len(mc.chain))
	for _, mw := range mc.chain {
		if mw.Name == mod.DependencyMiddlewareName {
			newChain = append(newChain, mod.Middlewares...)
		} else {
			newChain = append(newChain, mw)
		}
	}
	mc.chain = newChain
	return nil
}

func (mc *MiddlewareChain[T]) append(mod MiddlewareModification[T]) error {
	if err := mc.validate(mod); err != nil {
		return err
	}

	newChain := make([]ReferenceableMiddleware[T], 0, len(mc.chain))
	for _, mw := range mc.chain {
		newChain = append(newChain, mw)
		if mw.Name == mod.DependencyMiddlewareName {
			newChain = append(newChain, mod.Middlewares...)
		}
	}
	mc.chain = newChain
	return nil
}

func (mc *MiddlewareChain[T]) replaceAll(mod MiddlewareModification[T]) error {
	if err := mod.validate(); err != nil {
		return err
	}
	mc.chain = mod.Middlewares
	return nil
}

func (mc *MiddlewareChain[T]) validate(mod MiddlewareModification[T]) error {
	if err := mod.validate(); err != nil {
		return err
	}

	// prevent referencing non-existing middlewares
	existingNames := mc.Names()
	if !existingNames.Has(mod.DependencyMiddlewareName) {
		return fmt.Errorf("referenced dependency does not exist on chain: %s", mod.DependencyMiddlewareName)
	}

	// prevent appending/prepending a duplicate middleware
	for _, mw := range mod.Middlewares {
		if existingNames.Has(mw.Name) && mod.DependencyMiddlewareName == mw.Name && mod.Operation != OperationReplace {
			return fmt.Errorf("modification will cause a duplicate in chain: %s", mw.Name)
		}
	}

	// prevent replacing an internal middleware
	for _, mw := range mc.chain {
		if mw.Internal && mw.Name == mod.DependencyMiddlewareName && mod.Operation == OperationReplace {
			return fmt.Errorf("modification attempts to replace an internal middleware: %s", mw.Name)
		}
	}
	return nil
}

func (mc *MiddlewareChain[T]) modify(modifications ...MiddlewareModification[T]) error {
	for _, mod := range modifications {
		switch mod.Operation {
		case OperationPrepend:
			if err := mc.prepend(mod); err != nil {
				return err
			}
		case OperationReplace:
			if err := mc.replace(mod); err != nil {
				return err
			}
		case OperationReplaceAllUnsafe:
			if err := mc.replaceAll(mod); err != nil {
				return err
			}
		case OperationAppend:
			if err := mc.append(mod); err != nil {
				return err
			}
		}
	}
	return nil
}

type streamOrderAssertion struct {
	grpc.ServerStream
	name            string
	alreadyExecuted string
	notExecuted     string
}

func (o streamOrderAssertion) RecvMsg(m any) error {
	if err := mustHaveExecuted(o.Context(), streamExecuted{}, o.alreadyExecuted); err != nil {
		return err
	}

	if err := mustHaveNotExecuted(o.Context(), streamExecuted{}, o.notExecuted); err != nil {
		return err
	}

	mustMarkAsExecuted(o.Context(), streamExecuted{}, o.name)
	err := o.ServerStream.RecvMsg(m)
	return err
}

func (o streamOrderAssertion) SendMsg(m any) error {
	return o.ServerStream.SendMsg(m)
}

func NewStreamMiddleware() *StreamOrderEnforcerBuilder {
	return &StreamOrderEnforcerBuilder{}
}

type StreamOrderEnforcerBuilder struct {
	name                     string
	streamInterceptor        grpc.StreamServerInterceptor
	internal                 bool
	interceptorExecuted      string
	interceptorNotExecuted   string
	streamWrapperExecuted    string
	streamWrapperNotExecuted string
}

func (soeb *StreamOrderEnforcerBuilder) WithName(name string) *StreamOrderEnforcerBuilder {
	soeb.name = name
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) WithInterceptor(interceptor grpc.StreamServerInterceptor) *StreamOrderEnforcerBuilder {
	soeb.streamInterceptor = interceptor
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) WithInternal(internal bool) *StreamOrderEnforcerBuilder {
	soeb.internal = internal
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) EnsureWrapperAlreadyExecuted(name string) *StreamOrderEnforcerBuilder {
	soeb.streamWrapperExecuted = name
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) EnsureWrapperNotExecuted(name string) *StreamOrderEnforcerBuilder {
	soeb.streamWrapperNotExecuted = name
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) EnsureInterceptorAlreadyExecuted(name string) *StreamOrderEnforcerBuilder {
	soeb.interceptorExecuted = name
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) EnsureInterceptorNotExecuted(name string) *StreamOrderEnforcerBuilder {
	soeb.interceptorNotExecuted = name
	return soeb
}

func (soeb *StreamOrderEnforcerBuilder) Done() ReferenceableMiddleware[grpc.StreamServerInterceptor] {
	if !spiceerrors.IsInTests() {
		return ReferenceableMiddleware[grpc.StreamServerInterceptor]{
			Name:       soeb.name,
			Internal:   soeb.internal,
			Middleware: soeb.streamInterceptor,
		}
	}

	return ReferenceableMiddleware[grpc.StreamServerInterceptor]{
		Name:     soeb.name,
		Internal: soeb.internal,
		Middleware: func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			wss := middleware.WrapServerStream(ss)
			if wss.WrappedContext.Value(streamExecuted{}) == nil {
				handle := executedHandle{executed: make(map[string]struct{}, 0)}
				wss.WrappedContext = context.WithValue(wss.WrappedContext, streamExecuted{}, &handle)
			}
			if wss.WrappedContext.Value(interceptorsExecuted{}) == nil {
				handle := executedHandle{executed: make(map[string]struct{}, 0)}
				wss.WrappedContext = context.WithValue(wss.WrappedContext, interceptorsExecuted{}, &handle)
			}

			if err := mustHaveExecuted(wss.WrappedContext, interceptorsExecuted{}, soeb.interceptorExecuted); err != nil {
				return err
			}

			if err := mustHaveNotExecuted(wss.WrappedContext, interceptorsExecuted{}, soeb.interceptorNotExecuted); err != nil {
				return err
			}

			mustMarkAsExecuted(wss.WrappedContext, interceptorsExecuted{}, soeb.name)

			wrappedStream := streamOrderAssertion{
				ServerStream:    wss,
				name:            soeb.name,
				alreadyExecuted: soeb.streamWrapperExecuted,
				notExecuted:     soeb.streamWrapperNotExecuted,
			}
			return soeb.streamInterceptor(srv, wrappedStream, info, handler)
		},
	}
}

func NewUnaryMiddleware() *UnaryOrderEnforcerBuilder {
	return &UnaryOrderEnforcerBuilder{}
}

type UnaryOrderEnforcerBuilder struct {
	name            string
	interceptor     grpc.UnaryServerInterceptor
	internal        bool
	alreadyExecuted string
	notExecuted     string
}

func (soeb *UnaryOrderEnforcerBuilder) WithName(name string) *UnaryOrderEnforcerBuilder {
	soeb.name = name
	return soeb
}

func (soeb *UnaryOrderEnforcerBuilder) WithInterceptor(interceptor grpc.UnaryServerInterceptor) *UnaryOrderEnforcerBuilder {
	soeb.interceptor = interceptor
	return soeb
}

func (soeb *UnaryOrderEnforcerBuilder) WithInternal(internal bool) *UnaryOrderEnforcerBuilder {
	soeb.internal = internal
	return soeb
}

func (soeb *UnaryOrderEnforcerBuilder) EnsureAlreadyExecuted(name string) *UnaryOrderEnforcerBuilder {
	soeb.alreadyExecuted = name
	return soeb
}

func (soeb *UnaryOrderEnforcerBuilder) EnsureNotExecuted(name string) *UnaryOrderEnforcerBuilder {
	soeb.notExecuted = name
	return soeb
}

func (soeb *UnaryOrderEnforcerBuilder) Done() ReferenceableMiddleware[grpc.UnaryServerInterceptor] {
	if !spiceerrors.IsInTests() {
		return ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			Name:       soeb.name,
			Internal:   soeb.internal,
			Middleware: soeb.interceptor,
		}
	}

	return ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		Name:     soeb.name,
		Internal: soeb.internal,
		Middleware: func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			if ctx.Value(interceptorsExecuted{}) == nil {
				handle := executedHandle{executed: make(map[string]struct{}, 0)}
				ctx = context.WithValue(ctx, interceptorsExecuted{}, &handle)
			}

			if err := mustHaveExecuted(ctx, interceptorsExecuted{}, soeb.alreadyExecuted); err != nil {
				return nil, err
			}

			if err := mustHaveNotExecuted(ctx, interceptorsExecuted{}, soeb.notExecuted); err != nil {
				return nil, err
			}

			mustMarkAsExecuted(ctx, interceptorsExecuted{}, soeb.name)
			return soeb.interceptor(ctx, req, info, handler)
		},
	}
}

func mustHaveNotExecuted(ctx context.Context, handleKey any, notExecuted string) error {
	if notExecuted == "" {
		return nil
	}

	val := ctx.Value(handleKey)
	if val == nil {
		return fmt.Errorf("interception order validation bookkeeping not present in context")
	}

	handle := val.(*executedHandle)
	if _, ok := handle.executed[notExecuted]; ok {
		return fmt.Errorf("expected interceptor %s to be not already executed", notExecuted)
	}

	return nil
}

func mustHaveExecuted(ctx context.Context, handleKey any, expectedExecuted string) error {
	if expectedExecuted == "" {
		return nil
	}

	val := ctx.Value(handleKey)
	if val == nil {
		return spiceerrors.MustBugf("interception order validation bookkeeping not present in context")
	}

	handle := val.(*executedHandle)
	if _, ok := handle.executed[expectedExecuted]; ok {
		return nil
	}

	return fmt.Errorf("expected interceptor %s to be already executed", expectedExecuted)
}

func mustMarkAsExecuted(ctx context.Context, handleKey any, name string) {
	val := ctx.Value(handleKey)
	if val == nil {
		panic("handle should exist")
	} else {
		handle := val.(*executedHandle)
		handle.executed[name] = struct{}{}
	}
}

type executedHandle struct {
	executed map[string]struct{}
}

type interceptorsExecuted struct{}

type streamExecuted struct{}
