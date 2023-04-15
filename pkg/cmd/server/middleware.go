package server

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/util"

	"google.golang.org/grpc"
)

// MiddlewareChain describes an ordered sequence of middlewares that can be modified
// with one or more MiddlewareModification. This struct is used to facilitate the
// creation and modification of gRPC middleware chains
type MiddlewareChain struct {
	chain []ReferenceableMiddleware
}

// NewMiddlewareChain creates a new middleware chain given zero or more named middlewares.
// An error will be returned in case validation of the NamedMiddlewares fail.
func NewMiddlewareChain(mw ...ReferenceableMiddleware) (MiddlewareChain, error) {
	if err := validate(mw); err != nil {
		return MiddlewareChain{}, err
	}
	return MiddlewareChain{chain: mw}, nil
}

// MiddlewareModification describes an operation to modify a MiddlewareChain
type MiddlewareModification struct {
	// DependencyMiddlewareName is used to define with respect to which middleware an operation is performed.
	// Dependency is not required for ReplaceAll operation
	DependencyMiddlewareName string

	// Operation describes the type of operation to be performed
	Operation MiddlewareOperation

	// Middlewares are the named middlewares that will be part of this modification
	Middlewares []ReferenceableMiddleware
}

func (mm MiddlewareModification) validate() error {
	if mm.Operation != OperationReplaceAllUnsafe && mm.DependencyMiddlewareName == "" {
		return fmt.Errorf("cannot perform middleware modification without a dependency: %v", mm)
	}
	return validate(mm.Middlewares)
}

func validate(mws []ReferenceableMiddleware) error {
	names := util.NewSet[string]()
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
type ReferenceableMiddleware struct {
	Name                string
	Internal            bool
	UnaryMiddleware     grpc.UnaryServerInterceptor
	StreamingMiddleware grpc.StreamServerInterceptor
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
func (mc *MiddlewareChain) Names() *util.Set[string] {
	names := util.NewSet[string]()
	for _, mw := range mc.chain {
		names.Add(mw.Name)
	}
	return names
}

// ToGRPCInterceptors generates slices of gRPC interceptors ready to be installed in a server
func (mc *MiddlewareChain) ToGRPCInterceptors() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0, len(mc.chain))
	streamingInterceptors := make([]grpc.StreamServerInterceptor, 0, len(mc.chain))
	for _, mw := range mc.chain {
		unaryInterceptors = append(unaryInterceptors, mw.UnaryMiddleware)
		streamingInterceptors = append(streamingInterceptors, mw.StreamingMiddleware)
	}
	return unaryInterceptors, streamingInterceptors
}

func (mc *MiddlewareChain) prepend(mod MiddlewareModification) error {
	if err := mc.validate(mod); err != nil {
		return err
	}

	newChain := make([]ReferenceableMiddleware, 0, len(mc.chain))
	for _, mw := range mc.chain {
		if mw.Name == mod.DependencyMiddlewareName {
			newChain = append(newChain, mod.Middlewares...)
		}
		newChain = append(newChain, mw)
	}
	mc.chain = newChain
	return nil
}

func (mc *MiddlewareChain) replace(mod MiddlewareModification) error {
	if err := mc.validate(mod); err != nil {
		return err
	}
	newChain := make([]ReferenceableMiddleware, 0, len(mc.chain))
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

func (mc *MiddlewareChain) append(mod MiddlewareModification) error {
	if err := mc.validate(mod); err != nil {
		return err
	}

	newChain := make([]ReferenceableMiddleware, 0, len(mc.chain))
	for _, mw := range mc.chain {
		newChain = append(newChain, mw)
		if mw.Name == mod.DependencyMiddlewareName {
			newChain = append(newChain, mod.Middlewares...)
		}
	}
	mc.chain = newChain
	return nil
}

func (mc *MiddlewareChain) replaceAll(mod MiddlewareModification) error {
	if err := mod.validate(); err != nil {
		return err
	}
	mc.chain = mod.Middlewares
	return nil
}

func (mc *MiddlewareChain) validate(mod MiddlewareModification) error {
	if err := mod.validate(); err != nil {
		return err
	}

	// prevent referencing non-existing middlewares
	existingNames := mc.Names()
	if !existingNames.Has(mod.DependencyMiddlewareName) {
		return fmt.Errorf("referenced dependency does not exist on chain: %s", mod.DependencyMiddlewareName)
	}

	// prevent appending/prepending a duplicate middleware
	for _, middleware := range mod.Middlewares {
		if existingNames.Has(middleware.Name) && mod.DependencyMiddlewareName == middleware.Name && mod.Operation != OperationReplace {
			return fmt.Errorf("modification will cause a duplicate in chain: %s", middleware.Name)
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

func (mc *MiddlewareChain) modify(modifications ...MiddlewareModification) error {
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
