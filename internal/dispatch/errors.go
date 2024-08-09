package dispatch

import (
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// MaxDepthExceededError is an error returned when the maximum depth for dispatching has been exceeded.
type MaxDepthExceededError struct {
	error

	// Request is the request that exceeded the maximum depth.
	Request DispatchableRequest
}

// NewMaxDepthExceededError creates a new MaxDepthExceededError.
func NewMaxDepthExceededError(req DispatchableRequest) error {
	return MaxDepthExceededError{
		fmt.Errorf("max depth exceeded: this usually indicates a recursive or too deep data dependency. See: https://spicedb.dev/d/debug-max-depth"),
		req,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err MaxDepthExceededError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.ResourceExhausted,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_MAXIMUM_DEPTH_EXCEEDED,
			map[string]string{},
		),
	)
}
