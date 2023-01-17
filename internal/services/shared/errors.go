package shared

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
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

// NewSchemaWriteDataValidationError creates a new error representing that a schema write cannot be
// completed due to existing data that would be left unreferenced.
func NewSchemaWriteDataValidationError(message string, args ...any) ErrSchemaWriteDataValidation {
	return ErrSchemaWriteDataValidation{
		error: fmt.Errorf(message, args...),
	}
}

// ErrSchemaWriteDataValidation occurs when a schema cannot be applied due to leaving data unreferenced.
type ErrSchemaWriteDataValidation struct {
	error
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrSchemaWriteDataValidation) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrSchemaWriteDataValidation) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_SCHEMA_TYPE_ERROR,
			map[string]string{},
		),
	)
}

func AsValidationError(err error) *ErrSchemaWriteDataValidation {
	var validationErr ErrSchemaWriteDataValidation
	if errors.As(err, &validationErr) {
		return &validationErr
	}
	return nil
}
