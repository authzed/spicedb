package v1

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ErrExceedsMaximumUpdates occurs when too many updates are given to a call.
type ErrExceedsMaximumUpdates struct {
	error
	updateCount     uint16
	maxCountAllowed uint16
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrExceedsMaximumUpdates) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Uint16("updateCount", err.updateCount).Uint16("maxCountAllowed", err.maxCountAllowed)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrExceedsMaximumUpdates) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_TOO_MANY_UPDATES_IN_REQUEST,
			map[string]string{
				"update_count":            strconv.Itoa(int(err.updateCount)),
				"maximum_updates_allowed": strconv.Itoa(int(err.maxCountAllowed)),
			},
		),
	)
}

// NewExceedsMaximumUpdatesErr creates a new error representing that too many updates were given to a WriteRelationships call.
func NewExceedsMaximumUpdatesErr(updateCount uint16, maxCountAllowed uint16) ErrExceedsMaximumUpdates {
	return ErrExceedsMaximumUpdates{
		error:           fmt.Errorf("update count of %d is greater than maximum allowed of %d", updateCount, maxCountAllowed),
		updateCount:     updateCount,
		maxCountAllowed: maxCountAllowed,
	}
}

// ErrExceedsMaximumPreconditions occurs when too many preconditions are given to a call.
type ErrExceedsMaximumPreconditions struct {
	error
	preconditionCount uint16
	maxCountAllowed   uint16
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrExceedsMaximumPreconditions) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Uint16("preconditionCount", err.preconditionCount).Uint16("maxCountAllowed", err.maxCountAllowed)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrExceedsMaximumPreconditions) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_TOO_MANY_PRECONDITIONS_IN_REQUEST,
			map[string]string{
				"precondition_count":      strconv.Itoa(int(err.preconditionCount)),
				"maximum_updates_allowed": strconv.Itoa(int(err.maxCountAllowed)),
			},
		),
	)
}

// NewExceedsMaximumPreconditionsErr creates a new error representing that too many preconditions were given to a call.
func NewExceedsMaximumPreconditionsErr(preconditionCount uint16, maxCountAllowed uint16) ErrExceedsMaximumPreconditions {
	return ErrExceedsMaximumPreconditions{
		error: fmt.Errorf(
			"precondition count of %d is greater than maximum allowed of %d",
			preconditionCount,
			maxCountAllowed),
		preconditionCount: preconditionCount,
		maxCountAllowed:   maxCountAllowed,
	}
}

// ErrPreconditionFailed occurs when the precondition to a write tuple call does not match.
type ErrPreconditionFailed struct {
	error
	precondition *v1.Precondition
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrPreconditionFailed) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Interface("precondition", err.precondition)
}

// NewPreconditionFailedErr constructs a new precondition failed error.
func NewPreconditionFailedErr(precondition *v1.Precondition) error {
	return ErrPreconditionFailed{
		error:        fmt.Errorf("unable to satisfy write precondition `%s`", precondition),
		precondition: precondition,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrPreconditionFailed) GRPCStatus() *status.Status {
	metadata := map[string]string{
		"precondition_resource_type": err.precondition.Filter.ResourceType,
		"precondition_operation":     v1.Precondition_Operation_name[int32(err.precondition.Operation)],
	}

	if err.precondition.Filter.OptionalResourceId != "" {
		metadata["precondition_resource_id"] = err.precondition.Filter.OptionalResourceId
	}

	if err.precondition.Filter.OptionalRelation != "" {
		metadata["precondition_relation"] = err.precondition.Filter.OptionalRelation
	}

	if err.precondition.Filter.OptionalSubjectFilter != nil {
		metadata["precondition_subject_type"] = err.precondition.Filter.OptionalSubjectFilter.SubjectType

		if err.precondition.Filter.OptionalSubjectFilter.OptionalSubjectId != "" {
			metadata["precondition_subject_id"] = err.precondition.Filter.OptionalSubjectFilter.OptionalSubjectId
		}

		if err.precondition.Filter.OptionalSubjectFilter.OptionalRelation != nil {
			metadata["precondition_subject_relation"] = err.precondition.Filter.OptionalSubjectFilter.OptionalRelation.Relation
		}
	}

	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_WRITE_OR_DELETE_PRECONDITION_FAILURE,
			metadata,
		),
	)
}

// ErrDuplicateRelationshipError indicates that an update was attempted on the same relationship.
type ErrDuplicateRelationshipError struct {
	error
	update *v1.RelationshipUpdate
}

// NewDuplicateRelationshipErr constructs a new invalid subject error.
func NewDuplicateRelationshipErr(update *v1.RelationshipUpdate) ErrDuplicateRelationshipError {
	return ErrDuplicateRelationshipError{
		error: fmt.Errorf(
			"found more than one update with relationship `%s` in this request; a relationship can only be specified in an update once per overall WriteRelationships request",
			tuple.MustRelString(update.Relationship),
		),
		update: update,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrDuplicateRelationshipError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_SUBJECT_TYPE,
			map[string]string{
				"definition_name": err.update.Relationship.Resource.ObjectType,
				"relationship":    tuple.MustRelString(err.update.Relationship),
			},
		),
	)
}

func rewriteError(ctx context.Context, err error) error {
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

	switch {
	case errors.As(err, &typeError):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_SCHEMA_TYPE_ERROR)
	case errors.As(err, &compilerError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR)
	case errors.As(err, &sourceError):
		return spiceerrors.WithCodeAndReason(err, codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR)

	case errors.As(err, &nsNotFoundError):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_DEFINITION)
	case errors.As(err, &relationNotFoundError):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return shared.ErrServiceReadOnly
	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid zedtoken: %s", err)
	case errors.As(err, &datastore.ErrReadOnly{}):
		return shared.ErrServiceReadOnly
	case errors.As(err, &datastore.ErrCaveatNameNotFound{}):
		return spiceerrors.WithCodeAndReason(err, codes.FailedPrecondition, v1.ErrorReason_ERROR_REASON_UNKNOWN_CAVEAT)

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
		return status.Errorf(codes.Canceled, "%s", err)
	default:
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}

type uinteger interface {
	uint32 | uint16
}

func defaultIfZero[T uinteger](value T, defaultValue T) T {
	if value == 0 {
		return defaultValue
	}
	return value
}
