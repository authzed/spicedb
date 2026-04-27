package dispatch

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/sharederrors"
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
		errors.New("max depth exceeded: this usually indicates a recursive or too deep data dependency. See " + sharederrors.MaxDepthErrorLink),
		req,
	}
}

// IsMaxDepthExceeded returns true if err represents a max-depth-exceeded condition,
// whether it is an in-process MaxDepthExceededError or a gRPC status error that crossed
// a network boundary (identified by ERROR_REASON_MAXIMUM_DEPTH_EXCEEDED in ErrorInfo).
func IsMaxDepthExceeded(err error) bool {
	var maxDepthErr MaxDepthExceededError
	if errors.As(err, &maxDepthErr) {
		return true
	}
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.FailedPrecondition {
		return false
	}
	for _, detail := range s.Details() {
		if errInfo, ok := detail.(*errdetails.ErrorInfo); ok {
			if errInfo.Reason == v1.ErrorReason_ERROR_REASON_MAXIMUM_DEPTH_EXCEEDED.String() {
				return true
			}
		}
	}
	return false
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err MaxDepthExceededError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_MAXIMUM_DEPTH_EXCEEDED,
			map[string]string{},
		),
	)
}
