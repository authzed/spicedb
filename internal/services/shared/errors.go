package shared

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/rs/zerolog"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
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

// MaxDepthExceededError is an error returned when the maximum depth for dispatching has been exceeded.
type MaxDepthExceededError struct {
	error

	// AllowedMaximumDepth is the configured allowed maximum depth.
	AllowedMaximumDepth uint32
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err MaxDepthExceededError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.ResourceExhausted,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_MAXIMUM_DEPTH_EXCEEDED,
			map[string]string{
				"maximum_depth_allowed": strconv.Itoa(int(err.AllowedMaximumDepth)),
			},
		),
	)
}

// NewMaxDepthExceededError creates a new MaxDepthExceededError.
func NewMaxDepthExceededError(allowedMaximumDepth uint32, isCheckRequest bool) error {
	if isCheckRequest {
		return MaxDepthExceededError{
			fmt.Errorf("the check request has exceeded the allowable maximum depth of %d: this usually indicates a recursive or too deep data dependency. Try running zed with --explain to see the dependency. See: https://spicedb.dev/d/debug-max-depth-check", allowedMaximumDepth),
			allowedMaximumDepth,
		}
	}

	return MaxDepthExceededError{
		fmt.Errorf("the request has exceeded the allowable maximum depth of %d: this usually indicates a recursive or too deep data dependency. See: https://spicedb.dev/d/debug-max-depth", allowedMaximumDepth),
		allowedMaximumDepth,
	}
}

func AsValidationError(err error) *ErrSchemaWriteDataValidation {
	var validationErr ErrSchemaWriteDataValidation
	if errors.As(err, &validationErr) {
		return &validationErr
	}
	return nil
}

type ConfigForErrors struct {
	MaximumAPIDepth uint32
}

func RewriteError(ctx context.Context, err error, config *ConfigForErrors) error {
	// Check if the error can be directly used.
	if _, ok := status.FromError(err); ok {
		return err
	}

	// Otherwise, convert any graph/datastore errors.
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var relationNotFoundError sharederrors.UnknownRelationError

	var compilerError compiler.BaseCompilerError
	var sourceError spiceerrors.ErrorWithSource
	var typeError namespace.TypeError
	var maxDepthError dispatch.MaxDepthExceededError

	switch {
	case errors.As(err, &typeError):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_SCHEMA_TYPE_ERROR)
	case errors.As(err, &compilerError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR)
	case errors.As(err, &sourceError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR)

	case errors.Is(err, cursor.ErrHashMismatch):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_INVALID_CURSOR)

	case errors.As(err, &nsNotFoundError):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_DEFINITION)
	case errors.As(err, &relationNotFoundError):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION)

	case errors.As(err, &maxDepthError):
		if config == nil {
			return spiceerrors.MustBugf("missing config for API error")
		}

		_, isCheckRequest := maxDepthError.Request.(*dispatchv1.DispatchCheckRequest)
		return NewMaxDepthExceededError(config.MaximumAPIDepth, isCheckRequest)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return ErrServiceReadOnly
	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid zedtoken: %s", err)
	case errors.As(err, &datastore.ErrReadOnly{}):
		return ErrServiceReadOnly
	case errors.As(err, &datastore.ErrCaveatNameNotFound{}):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_CAVEAT)
	case errors.As(err, &datastore.ErrWatchDisabled{}):
		return status.Errorf(codes.FailedPrecondition, "%s", err)

	case errors.As(err, &graph.ErrInvalidArgument{}):
		return status.Errorf(codes.InvalidArgument, "%s", err)
	case errors.As(err, &graph.ErrRequestCanceled{}):
		return status.Errorf(codes.Canceled, "request canceled: %s", err)
	case errors.As(err, &graph.ErrRelationMissingTypeInfo{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)
	case errors.As(err, &graph.ErrAlwaysFail{}):
		log.Ctx(ctx).Err(err).Msg("received internal error")
		return status.Errorf(codes.Internal, "internal error: %s", err)
	case errors.As(err, &graph.ErrUnimplemented{}):
		return status.Errorf(codes.Unimplemented, "%s", err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s", err)
	case errors.Is(err, context.Canceled):
		err := context.Cause(ctx)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
		}

		return status.Errorf(codes.Canceled, "%s", err)
	default:
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
