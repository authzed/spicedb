package dispatch

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type DispatchStream[T any] interface {
	Send(T) error
	grpc.ServerStream
}

// CollectingDispatchStream is a dispatch stream that collects results in memory.
type CollectingDispatchStream[T any] struct {
	Ctx     context.Context
	results []T
}

func (cc *CollectingDispatchStream[T]) Results() []T {
	return cc.results
}

func (cc *CollectingDispatchStream[T]) Send(result T) error {
	cc.results = append(cc.results, result)
	return nil
}

func (cc *CollectingDispatchStream[T]) SetHeader(metadata.MD) error {
	return nil
}

func (cc *CollectingDispatchStream[T]) SendHeader(metadata.MD) error {
	return nil
}

func (cc *CollectingDispatchStream[T]) SetTrailer(metadata.MD) {
}

func (cc *CollectingDispatchStream[T]) Context() context.Context {
	return cc.Ctx
}

func (cc *CollectingDispatchStream[T]) SendMsg(m interface{}) error {
	return fmt.Errorf("not implemented")
}

func (cc *CollectingDispatchStream[T]) RecvMsg(m interface{}) error {
	return fmt.Errorf("not implemented")
}

// WrappedDispatchStream is a dispatch stream that wraps another dispatch stream, and performs
// an operation on each result before puppeting back up to the parent stream.
type WrappedDispatchStream[T any] struct {
	DispatchStream[T]
	Ctx       context.Context
	Processor func(result T) (T, error)
}

func (wds *WrappedDispatchStream[T]) Send(result T) error {
	if wds.Processor == nil {
		return wds.DispatchStream.Send(result)
	}

	processed, err := wds.Processor(result)
	if err != nil {
		return err
	}
	return wds.DispatchStream.Send(processed)
}

func (wds *WrappedDispatchStream[T]) Context() context.Context {
	return wds.Ctx
}

// DispatchStreamWithContext returns the given dispatch stream, wrapped to return the given context.
func DispatchStreamWithContext[T any](stream DispatchStream[T], context context.Context) DispatchStream[T] {
	return &WrappedDispatchStream[T]{
		DispatchStream: stream,
		Ctx:            context,
		Processor:      nil,
	}
}
