package v1

import (
	"fmt"
	"strconv"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

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
			tuple.StringRelationshipWithoutCaveat(update.Relationship),
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
			v1.ErrorReason_ERROR_REASON_UPDATES_ON_SAME_RELATIONSHIP,
			map[string]string{
				"definition_name": err.update.Relationship.Resource.ObjectType,
				"relationship":    tuple.MustRelString(err.update.Relationship),
			},
		),
	)
}

// ErrMaxRelationshipContextError indicates an attempt to write a relationship that exceeded the maximum
// configured context size.
type ErrMaxRelationshipContextError struct {
	error
	update         *v1.RelationshipUpdate
	maxAllowedSize int
}

// NewMaxRelationshipContextError constructs a new max relationship context error.
func NewMaxRelationshipContextError(update *v1.RelationshipUpdate, maxAllowedSize int) ErrMaxRelationshipContextError {
	return ErrMaxRelationshipContextError{
		error: fmt.Errorf(
			"provided relationship `%s` exceeded maximum allowed caveat size of %d",
			tuple.StringRelationshipWithoutCaveat(update.Relationship),
			maxAllowedSize,
		),
		update:         update,
		maxAllowedSize: maxAllowedSize,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrMaxRelationshipContextError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_MAX_RELATIONSHIP_CONTEXT_SIZE,
			map[string]string{
				"relationship":     tuple.StringRelationshipWithoutCaveat(err.update.Relationship),
				"max_allowed_size": strconv.Itoa(err.maxAllowedSize),
				"context_size":     strconv.Itoa(proto.Size(err.update.Relationship)),
			},
		),
	)
}

// ErrCouldNotTransactionallyDelete indicates that a deletion could not occur transactionally.
type ErrCouldNotTransactionallyDelete struct {
	error
	limit  uint32
	filter *v1.RelationshipFilter
}

// NewCouldNotTransactionallyDeleteErr constructs a new could not transactionally deleter error.
func NewCouldNotTransactionallyDeleteErr(filter *v1.RelationshipFilter, limit uint32) ErrCouldNotTransactionallyDelete {
	return ErrCouldNotTransactionallyDelete{
		error: fmt.Errorf(
			"found more than %d relationships to be deleted and partial deletion was not requested",
			limit,
		),
		limit:  limit,
		filter: filter,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrCouldNotTransactionallyDelete) GRPCStatus() *status.Status {
	metadata := map[string]string{
		"limit":                strconv.Itoa(int(err.limit)),
		"filter_resource_type": err.filter.ResourceType,
	}

	if err.filter.OptionalResourceId != "" {
		metadata["filter_resource_id"] = err.filter.OptionalResourceId
	}

	if err.filter.OptionalRelation != "" {
		metadata["filter_relation"] = err.filter.OptionalRelation
	}

	if err.filter.OptionalSubjectFilter != nil {
		metadata["filter_subject_type"] = err.filter.OptionalSubjectFilter.SubjectType

		if err.filter.OptionalSubjectFilter.OptionalSubjectId != "" {
			metadata["filter_subject_id"] = err.filter.OptionalSubjectFilter.OptionalSubjectId
		}

		if err.filter.OptionalSubjectFilter.OptionalRelation != nil {
			metadata["filter_subject_relation"] = err.filter.OptionalSubjectFilter.OptionalRelation.Relation
		}
	}

	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_TOO_MANY_RELATIONSHIPS_FOR_TRANSACTIONAL_DELETE,
			metadata,
		),
	)
}

// ErrInvalidCursor indicates that an invalid cursor was found.
type ErrInvalidCursor struct {
	error
	reason string
}

// NewInvalidCursorErr constructs a new invalid cursor error.
func NewInvalidCursorErr(reason string) ErrInvalidCursor {
	return ErrInvalidCursor{
		error: fmt.Errorf(
			"the cursor provided is not valid: %s",
			reason,
		),
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrInvalidCursor) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_CURSOR,
			map[string]string{
				"reason": err.reason,
			},
		),
	)
}

func defaultIfZero[T comparable](value T, defaultValue T) T {
	var zero T
	if value == zero {
		return defaultValue
	}
	return value
}
