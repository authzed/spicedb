package dispatch

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/sharederrors"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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

// MaxDepthWithTraceError wraps the original max depth error with a snapshot
// of the traversal trace at the exact point of failure.
type MaxDepthWithTraceError struct {
	Err   error
	Trace *dispatchv1.LookupDebugTrace
}

func (m MaxDepthWithTraceError) Error() string {
	return m.Err.Error()
}

func (m MaxDepthWithTraceError) Unwrap() error {
	return m.Err
}
