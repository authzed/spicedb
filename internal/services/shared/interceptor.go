package shared

import (
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/middleware/servicespecific"
)

// WithUnaryServiceSpecificInterceptor is a helper to add a unary interceptor or interceptor
// chain to a service.
type WithUnaryServiceSpecificInterceptor struct {
	Unary grpc.UnaryServerInterceptor
}

// UnaryInterceptor implements servicespecific.ExtraUnaryInterceptor
func (wussi WithUnaryServiceSpecificInterceptor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return wussi.Unary
}

// WithStreamServiceSpecificInterceptor is a helper to add a stream interceptor or interceptor
// chain to a service.
type WithStreamServiceSpecificInterceptor struct {
	Stream grpc.StreamServerInterceptor
}

// StreamInterceptor implements servicespecific.ExtraStreamInterceptor
func (wsssi WithStreamServiceSpecificInterceptor) StreamInterceptor() grpc.StreamServerInterceptor {
	return wsssi.Stream
}

// WithServiceSpecificInterceptors is a helper to add both a unary and stream interceptor
// or interceptor chain to a service.
type WithServiceSpecificInterceptors struct {
	Unary  grpc.UnaryServerInterceptor
	Stream grpc.StreamServerInterceptor
}

// UnaryInterceptor implements servicespecific.ExtraUnaryInterceptor
func (wssi WithServiceSpecificInterceptors) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return wssi.Unary
}

// StreamInterceptor implements servicespecific.ExtraStreamInterceptor
func (wssi WithServiceSpecificInterceptors) StreamInterceptor() grpc.StreamServerInterceptor {
	return wssi.Stream
}

var (
	_ servicespecific.ExtraUnaryInterceptor  = WithUnaryServiceSpecificInterceptor{}
	_ servicespecific.ExtraUnaryInterceptor  = WithServiceSpecificInterceptors{}
	_ servicespecific.ExtraStreamInterceptor = WithServiceSpecificInterceptors{}
)
