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

// ErrExceedsMaximumLimit occurs when a limit that is too large is given to a call.
type ErrExceedsMaximumLimit struct {
	error
	providedLimit   uint64
	maxLimitAllowed uint64
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrExceedsMaximumLimit) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Uint64("providedLimit", err.providedLimit).Uint64("maxLimitAllowed", err.maxLimitAllowed)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrExceedsMaximumLimit) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_EXCEEDS_MAXIMUM_ALLOWABLE_LIMIT,
			map[string]string{
				"limit_provided":        strconv.FormatUint(err.providedLimit, 10),
				"maximum_limit_allowed": strconv.FormatUint(err.maxLimitAllowed, 10),
			},
		),
	)
}

// NewExceedsMaximumLimitErr creates a new error representing that the limit specified was too large.
func NewExceedsMaximumLimitErr(providedLimit uint64, maxLimitAllowed uint64) ErrExceedsMaximumLimit {
	return ErrExceedsMaximumLimit{
		error:           fmt.Errorf("provided limit %d is greater than maximum allowed of %d", providedLimit, maxLimitAllowed),
		providedLimit:   providedLimit,
		maxLimitAllowed: maxLimitAllowed,
	}
}

// ErrExceedsMaximumChecks occurs when too many checks are given to a call.
type ErrExceedsMaximumChecks struct {
	error
	checkCount      uint64
	maxCountAllowed uint64
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrExceedsMaximumChecks) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Uint64("checkCount", err.checkCount).Uint64("maxCountAllowed", err.maxCountAllowed)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrExceedsMaximumChecks) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_UNSPECIFIED,
			map[string]string{
				"check_count":            strconv.FormatUint(err.checkCount, 10),
				"maximum_checks_allowed": strconv.FormatUint(err.maxCountAllowed, 10),
			},
		),
	)
}

// NewExceedsMaximumChecksErr creates a new error representing that too many updates were given to a BulkCheckPermissions call.
func NewExceedsMaximumChecksErr(checkCount uint64, maxCountAllowed uint64) ErrExceedsMaximumChecks {
	return ErrExceedsMaximumChecks{
		error:           fmt.Errorf("check count of %d is greater than maximum allowed of %d", checkCount, maxCountAllowed),
		checkCount:      checkCount,
		maxCountAllowed: maxCountAllowed,
	}
}

// ErrExceedsMaximumUpdates occurs when too many updates are given to a call.
type ErrExceedsMaximumUpdates struct {
	error
	updateCount     uint64
	maxCountAllowed uint64
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrExceedsMaximumUpdates) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Uint64("updateCount", err.updateCount).Uint64("maxCountAllowed", err.maxCountAllowed)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrExceedsMaximumUpdates) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_TOO_MANY_UPDATES_IN_REQUEST,
			map[string]string{
				"update_count":            strconv.FormatUint(err.updateCount, 10),
				"maximum_updates_allowed": strconv.FormatUint(err.maxCountAllowed, 10),
			},
		),
	)
}

// NewExceedsMaximumUpdatesErr creates a new error representing that too many updates were given to a WriteRelationships call.
func NewExceedsMaximumUpdatesErr(updateCount uint64, maxCountAllowed uint64) ErrExceedsMaximumUpdates {
	return ErrExceedsMaximumUpdates{
		error:           fmt.Errorf("update count of %d is greater than maximum allowed of %d", updateCount, maxCountAllowed),
		updateCount:     updateCount,
		maxCountAllowed: maxCountAllowed,
	}
}

// ErrExceedsMaximumPreconditions occurs when too many preconditions are given to a call.
type ErrExceedsMaximumPreconditions struct {
	error
	preconditionCount uint64
	maxCountAllowed   uint64
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrExceedsMaximumPreconditions) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Uint64("preconditionCount", err.preconditionCount).Uint64("maxCountAllowed", err.maxCountAllowed)
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrExceedsMaximumPreconditions) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_TOO_MANY_PRECONDITIONS_IN_REQUEST,
			map[string]string{
				"precondition_count":      strconv.FormatUint(err.preconditionCount, 10),
				"maximum_updates_allowed": strconv.FormatUint(err.maxCountAllowed, 10),
			},
		),
	)
}

// NewExceedsMaximumPreconditionsErr creates a new error representing that too many preconditions were given to a call.
func NewExceedsMaximumPreconditionsErr(preconditionCount uint64, maxCountAllowed uint64) ErrExceedsMaximumPreconditions {
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
		"precondition_operation": v1.Precondition_Operation_name[int32(err.precondition.Operation)],
	}

	if err.precondition.Filter.ResourceType != "" {
		metadata["precondition_resource_type"] = err.precondition.Filter.ResourceType
	}

	if err.precondition.Filter.OptionalResourceId != "" {
		metadata["precondition_resource_id"] = err.precondition.Filter.OptionalResourceId
	}

	if err.precondition.Filter.OptionalResourceIdPrefix != "" {
		metadata["precondition_resource_id_prefix"] = err.precondition.Filter.OptionalResourceIdPrefix
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
			tuple.V1StringRelationshipWithoutCaveatOrExpiration(update.Relationship),
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
				"relationship":    tuple.MustV1StringRelationship(err.update.Relationship),
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
			tuple.V1StringRelationshipWithoutCaveatOrExpiration(update.Relationship),
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
				"relationship":     tuple.V1StringRelationshipWithoutCaveatOrExpiration(err.update.Relationship),
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

// ErrInvalidFilter indicates the specified relationship filter was invalid.
type ErrInvalidFilter struct {
	error

	filter string
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrInvalidFilter) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_FILTER,
			map[string]string{
				"filter": err.filter,
			},
		),
	)
}

// NewInvalidFilterErr constructs a new invalid filter error.
func NewInvalidFilterErr(reason string, filter string) ErrInvalidFilter {
	return ErrInvalidFilter{
		error: fmt.Errorf(
			"the relationship filter provided is not valid: %s", reason,
		),
		filter: filter,
	}
}

// NewEmptyPreconditionErr constructs a new empty precondition error.
func NewEmptyPreconditionErr() ErrEmptyPrecondition {
	return ErrEmptyPrecondition{
		error: fmt.Errorf(
			"one of the specified preconditions is empty",
		),
	}
}

// ErrEmptyPrecondition indicates an empty precondition was found.
type ErrEmptyPrecondition struct {
	error
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrEmptyPrecondition) GRPCStatus() *status.Status {
	// TODO(jschorr): Put a proper error reason in here.
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_UNSPECIFIED,
			map[string]string{},
		),
	)
}

// NewNotAPermissionError constructs a new not a permission error.
func NewNotAPermissionError(relationName string) ErrNotAPermission {
	return ErrNotAPermission{
		error: fmt.Errorf(
			"the relation `%s` is not a permission", relationName,
		),
		relationName: relationName,
	}
}

// ErrNotAPermission indicates that the relation is not a permission.
type ErrNotAPermission struct {
	error
	relationName string
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrNotAPermission) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION,
			map[string]string{
				"relationName": err.relationName,
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

// ErrTransactionMetadataTooLarge indicates that the metadata for a transaction is too large.
type ErrTransactionMetadataTooLarge struct {
	error
	metadataSize int
	maxSize      int
}

// NewTransactionMetadataTooLargeErr constructs a new transaction metadata too large error.
func NewTransactionMetadataTooLargeErr(metadataSize int, maxSize int) ErrTransactionMetadataTooLarge {
	return ErrTransactionMetadataTooLarge{
		error:        fmt.Errorf("metadata size of %d is greater than maximum allowed of %d", metadataSize, maxSize),
		metadataSize: metadataSize,
		maxSize:      maxSize,
	}
}

func (err ErrTransactionMetadataTooLarge) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Int("metadataSize", err.metadataSize).Int("maxSize", err.maxSize)
}

func (err ErrTransactionMetadataTooLarge) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_TRANSACTION_METADATA_TOO_LARGE,
			map[string]string{
				"metadata_byte_size":                 strconv.Itoa(err.metadataSize),
				"maximum_allowed_metadata_byte_size": strconv.Itoa(err.maxSize),
			},
		),
	)
}
