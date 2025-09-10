package dispatch

import (
	"context"
	"sync"
	"sync/atomic"

	grpc "google.golang.org/grpc"
)

// Stream defines the interface generically matching a streaming dispatch response.
type Stream[T any] interface {
	// Publish publishes the result to the stream.
	Publish(T) error

	// Context returns the context for the stream.
	Context() context.Context
}

type grpcStream[T any] interface {
	grpc.ServerStream
	Send(T) error
}

// WrapGRPCStream wraps a gRPC result stream with a concurrent-safe dispatch stream. This is
// necessary because gRPC response streams are *not concurrent safe*.
// See: https://groups.google.com/g/grpc-io/c/aI6L6M4fzQ0?pli=1
func WrapGRPCStream[R any, S grpcStream[R]](grpcStream S) Stream[R] {
	return &concurrentSafeStream[R]{
		grpcStream: grpcStream,
		mu:         sync.Mutex{},
	}
}

type concurrentSafeStream[T any] struct {
	grpcStream grpcStream[T] // GUARDED_BY(mu)
	mu         sync.Mutex
}

func (s *concurrentSafeStream[T]) Context() context.Context {
	return s.grpcStream.Context()
}

func (s *concurrentSafeStream[T]) Publish(result T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grpcStream.Send(result)
}

// NewCollectingDispatchStream creates a new CollectingDispatchStream.
func NewCollectingDispatchStream[T any](ctx context.Context) *CollectingDispatchStream[T] {
	return &CollectingDispatchStream[T]{
		ctx:     ctx,
		results: nil,
		mu:      sync.Mutex{},
	}
}

// CollectingDispatchStream is a dispatch stream that collects results in memory.
type CollectingDispatchStream[T any] struct {
	ctx     context.Context
	results []T // GUARDED_BY(mu)
	mu      sync.Mutex
}

func (s *CollectingDispatchStream[T]) Context() context.Context {
	return s.ctx
}

func (s *CollectingDispatchStream[T]) Results() []T {
	return s.results
}

func (s *CollectingDispatchStream[T]) Publish(result T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, result)
	return nil
}

// WrappedDispatchStream is a dispatch stream that wraps another dispatch stream, and performs
// an operation on each result before puppeting back up to the parent stream.
type WrappedDispatchStream[T any] struct {
	Stream    Stream[T]
	Ctx       context.Context
	Processor func(result T) (T, bool, error)
}

func (s *WrappedDispatchStream[T]) Publish(result T) error {
	if s.Processor == nil {
		return s.Stream.Publish(result)
	}

	processed, ok, err := s.Processor(result)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	return s.Stream.Publish(processed)
}

func (s *WrappedDispatchStream[T]) Context() context.Context {
	return s.Ctx
}

// StreamWithContext returns the given dispatch stream, wrapped to return the given context.
func StreamWithContext[T any](context context.Context, stream Stream[T]) Stream[T] {
	return &WrappedDispatchStream[T]{
		Stream:    stream,
		Ctx:       context,
		Processor: nil,
	}
}

// HandlingDispatchStream is a dispatch stream that executes a handler for each item published.
// It uses an internal mutex to ensure it is thread safe.
type HandlingDispatchStream[T any] struct {
	ctx       context.Context
	processor func(result T) error // GUARDED_BY(mu)
	mu        sync.Mutex
}

// NewHandlingDispatchStream returns a new handling dispatch stream.
func NewHandlingDispatchStream[T any](ctx context.Context, processor func(result T) error) Stream[T] {
	return &HandlingDispatchStream[T]{
		ctx:       ctx,
		processor: processor,
		mu:        sync.Mutex{},
	}
}

func (s *HandlingDispatchStream[T]) Publish(result T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.processor == nil {
		return nil
	}

	return s.processor(result)
}

func (s *HandlingDispatchStream[T]) Context() context.Context {
	return s.ctx
}

// CountingDispatchStream is a dispatch stream that counts the number of items published.
// It uses an internal atomic int to ensure it is thread safe.
type CountingDispatchStream[T any] struct {
	Stream Stream[T]
	count  *atomic.Uint64
}

func NewCountingDispatchStream[T any](wrapped Stream[T]) *CountingDispatchStream[T] {
	return &CountingDispatchStream[T]{
		Stream: wrapped,
		count:  &atomic.Uint64{},
	}
}

func (s *CountingDispatchStream[T]) PublishedCount() uint64 {
	return s.count.Load()
}

func (s *CountingDispatchStream[T]) Publish(result T) error {
	err := s.Stream.Publish(result)
	if err != nil {
		return err
	}

	s.count.Add(1)
	return nil
}

func (s *CountingDispatchStream[T]) Context() context.Context {
	return s.Stream.Context()
}

// VTCloneable is an interface that requires a CloneVT method.
type VTCloneable[T any] interface {
	CloneVT() T
}

// NewCloningCollectingDispatchStream creates a new CloningCollectingDispatchStream.
func NewCloningCollectingDispatchStream[T VTCloneable[T]](ctx context.Context) *CloningCollectingDispatchStream[T] {
	return &CloningCollectingDispatchStream[T]{
		ctx:     ctx,
		results: nil,
		mu:      sync.Mutex{},
	}
}

// CloningCollectingDispatchStream is a dispatch stream that collects results in memory, cloning
// when publishing to avoid mutation issues.
type CloningCollectingDispatchStream[T VTCloneable[T]] struct {
	ctx     context.Context
	results []T // GUARDED_BY(mu)
	mu      sync.Mutex
}

func (s *CloningCollectingDispatchStream[T]) Context() context.Context {
	return s.ctx
}

func (s *CloningCollectingDispatchStream[T]) Results() []T {
	return s.results
}

func (s *CloningCollectingDispatchStream[T]) Publish(result T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, result.CloneVT())
	return nil
}

type fakeMessage struct{}

func (m *fakeMessage) CloneVT() *fakeMessage {
	return &fakeMessage{}
}

// Ensure the streams implement the interface.
var (
	_ Stream[any]          = &CollectingDispatchStream[any]{}
	_ Stream[any]          = &WrappedDispatchStream[any]{}
	_ Stream[any]          = &CountingDispatchStream[any]{}
	_ Stream[any]          = &HandlingDispatchStream[any]{}
	_ Stream[*fakeMessage] = &CloningCollectingDispatchStream[*fakeMessage]{}
)
