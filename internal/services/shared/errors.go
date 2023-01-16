package shared

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ErrServiceReadOnly is an extended GRPC error returned when a service is in read-only mode.
var ErrServiceReadOnly = mustMakeStatusReadonly()

func mustMakeStatusReadonly() error {
	status, err := status.New(codes.Unavailable, "service read-only").WithDetails(&errdetails.ErrorInfo{
		Reason: v1.ErrorReason_name[int32(v1.ErrorReason_ERROR_REASON_SERVICE_READ_ONLY)],
		Domain: spiceerrors.Domain,
	})
	if err != nil {
		panic("error constructing shared error type")
	}
	return status.Err()
}
