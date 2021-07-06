package serviceerrors

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ReasonReadOnly is the error reason that will show up in ErrorInfo when the service is in
	// read-only mode.
	ReasonReadOnly = "SERVICE_READ_ONLY"
)

// ErrServiceReadOnly is an extended GRPC error returned when a service is in read-only mode.
var ErrServiceReadOnly = mustMakeStatusReadonly()

func mustMakeStatusReadonly() error {
	status, err := status.New(codes.Unavailable, "service read-only").WithDetails(&errdetails.ErrorInfo{
		Reason: ReasonReadOnly,
		Domain: "authzed.com",
	})
	if err != nil {
		panic("error constructing shared error type")
	}
	return status.Err()
}
