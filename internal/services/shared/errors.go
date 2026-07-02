package shared

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// MySQL server error numbers for transient/retryable failures. Mirrors the values in the
// mysql datastore package (which are unexported).
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
const (
	mysqlErrLockWaitTimeout = 1205
	mysqlErrDeadlock        = 1213
)

const genericDatastoreErrorMessage = "an internal error occurred while accessing the datastore"

// sanitizeDatastoreDriverError converts a raw datastore driver error — which may embed
// engine-specific internals such as SQLSTATE codes — into a descriptive, engine-agnostic
// gRPC status. It returns (status, true) when err is a recognized raw driver error, and
// (nil, false) otherwise. The full error is logged server-side for diagnosis.
func sanitizeDatastoreDriverError(ctx context.Context, err error) (error, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		log.Ctx(ctx).Err(err).Msg("received internal datastore error")
		switch {
		case pgxcommon.IsQueryCanceledError(err):
			return status.Error(codes.Canceled, "the datastore operation was canceled"), true
		case pgxcommon.IsSerializationError(err):
			return status.Error(codes.Aborted, "the operation conflicted with a concurrent transaction; please retry"), true
		default:
			return status.Error(codes.Internal, genericDatastoreErrorMessage), true
		}
	}

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		log.Ctx(ctx).Err(err).Msg("received internal datastore error")
		switch mysqlErr.Number {
		case mysqlErrDeadlock, mysqlErrLockWaitTimeout:
			return status.Error(codes.Aborted, "the operation conflicted with a concurrent transaction; please retry"), true
		default:
			return status.Error(codes.Internal, genericDatastoreErrorMessage), true
		}
	}

	return nil, false
}

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
func NewSchemaWriteDataValidationError(message string, args []any, metadata map[string]string) SchemaWriteDataValidationError {
	return SchemaWriteDataValidationError{
		error:    fmt.Errorf(message, args...),
		metadata: metadata,
	}
}

// SchemaWriteDataValidationError occurs when a schema cannot be applied due to leaving data unreferenced.
type SchemaWriteDataValidationError struct {
	error
	metadata map[string]string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err SchemaWriteDataValidationError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err SchemaWriteDataValidationError) GRPCStatus() *status.Status {
	if err.metadata == nil {
		err.metadata = map[string]string{}
	}
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_SCHEMA_TYPE_ERROR,
			err.metadata,
		),
	)
}

// MaxDepthExceededError is an error returned when the maximum depth for dispatching has been exceeded.
type MaxDepthExceededError struct {
	*spiceerrors.WithAdditionalDetailsError

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
			err.AddToDetails(map[string]string{
				"maximum_depth_allowed": strconv.Itoa(int(err.AllowedMaximumDepth)),
			}),
		),
	)
}

func AsValidationError(err error) *SchemaWriteDataValidationError {
	var validationErr SchemaWriteDataValidationError
	if errors.As(err, &validationErr) {
		return &validationErr
	}
	return nil
}

type ConfigForErrors struct {
	MaximumAPIDepth uint32
	DebugTrace      *v1.DebugInformation
}

func RewriteErrorWithoutConfig(ctx context.Context, err error) error {
	return rewriteError(ctx, err, nil)
}

func RewriteError(ctx context.Context, err error, config *ConfigForErrors) error {
	rerr := rewriteError(ctx, err, config)
	if config != nil && config.DebugTrace != nil {
		spiceerrors.WithAdditionalDetails(rerr, spiceerrors.DebugTraceErrorDetailsKey, config.DebugTrace.String())
	}
	return rerr
}

func rewriteError(ctx context.Context, err error, _ *ConfigForErrors) error {
	// Check if the error can be directly used.
	if _, ok := status.FromError(err); ok {
		return err
	}

	// Otherwise, convert any graph/datastore errors.
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var relationNotFoundError sharederrors.UnknownRelationError

	var compilerError compiler.BaseCompilerError
	var sourceError *spiceerrors.WithSourceError
	var typeError schema.TypeError

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

	case errors.As(err, &datastore.ReadOnlyError{}):
		return ErrServiceReadOnly
	case errors.As(err, &datastore.InvalidRevisionError{}):
		return status.Errorf(codes.OutOfRange, "invalid zedtoken: %s", err)
	case errors.As(err, &datastore.CaveatNameNotFoundError{}):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_CAVEAT)
	case errors.Is(err, datastore.ErrSchemaHashPreconditionFailed):
		return status.Errorf(codes.FailedPrecondition, "%s", err)
	case errors.Is(err, datastore.ErrSchemaNotFound):
		return status.Errorf(codes.NotFound, "%s", err)
	case errors.As(err, &datastore.SchemaNotDefinedError{}):
		return status.Errorf(codes.NotFound, "%s", err)
	case errors.As(err, &datastore.WatchDisabledError{}):
		return status.Errorf(codes.FailedPrecondition, "%s", err)
	case errors.As(err, &datastore.WatchCanceledError{}):
		return status.Errorf(codes.Canceled, "watch canceled by user: %s", err)
	case errors.As(err, &datastore.WatchDisconnectedError{}):
		return status.Errorf(codes.ResourceExhausted, "watch disconnected: %s", err)
	case errors.As(err, &datastore.WatchRetryableError{}):
		// Unavailable is safe to retry
		return status.Error(codes.Unavailable, err.Error())
	case errors.As(err, &datastore.CounterAlreadyRegisteredError{}):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_COUNTER_ALREADY_REGISTERED)
	case errors.As(err, &datastore.CounterNotRegisteredError{}):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_COUNTER_NOT_REGISTERED)

	case errors.As(err, &graph.RelationMissingTypeInfoError{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)
	case errors.As(err, &graph.AlwaysFailError{}):
		log.Ctx(ctx).Err(err).Msg("received internal error")
		return status.Errorf(codes.Internal, "internal error: %s", err)
	case errors.As(err, &graph.UnimplementedError{}):
		return status.Errorf(codes.Unimplemented, "%s", err)
	case errors.Is(err, pool.ErrAcquire):
		return status.Errorf(codes.ResourceExhausted, "%s: consider increasing write pool size and/or datastore capacity", err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s", err)
	case errors.Is(err, context.Canceled):
		err := context.Cause(ctx)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			return status.Errorf(codes.Canceled, "%s", err)
		}

		return status.Errorf(codes.Canceled, "context canceled")
	default:
		if sanitized, ok := sanitizeDatastoreDriverError(ctx, err); ok {
			return sanitized
		}
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
