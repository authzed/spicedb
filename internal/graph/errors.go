package graph

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/sharederrors"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ErrRequestCanceled occurs when a request has been canceled.
type ErrRequestCanceled struct {
	error
}

// NewRequestCanceledErr constructs a new request was canceled error.
func NewRequestCanceledErr() error {
	return ErrRequestCanceled{
		error: errors.New("request canceled"),
	}
}

// ErrCheckFailure occurs when check failed in some manner. Note this should not apply to
// namespaces and relations not being found.
type ErrCheckFailure struct {
	error
}

// NewCheckFailureErr constructs a new check failed error.
func NewCheckFailureErr(baseErr error) error {
	return ErrCheckFailure{
		error: fmt.Errorf("error performing check: %w", baseErr),
	}
}

// ErrExpansionFailure occurs when expansion failed in some manner. Note this should not apply to
// namespaces and relations not being found.
type ErrExpansionFailure struct {
	error
}

// NewExpansionFailureErr constructs a new expansion failed error.
func NewExpansionFailureErr(baseErr error) error {
	return ErrExpansionFailure{
		error: fmt.Errorf("error performing expand: %w", baseErr),
	}
}

// ErrAlwaysFail is returned when an internal error leads to an operation
// guaranteed to fail.
type ErrAlwaysFail struct {
	error
}

// NewAlwaysFailErr constructs a new always fail error.
func NewAlwaysFailErr() error {
	return ErrAlwaysFail{
		error: errors.New("always fail"),
	}
}

// ErrRelationNotFound occurs when a relation was not found under a namespace.
type ErrRelationNotFound struct {
	error
	namespaceName string
	relationName  string
}

// NamespaceName returns the name of the namespace in which the relation was not found.
func (err ErrRelationNotFound) NamespaceName() string {
	return err.namespaceName
}

// NotFoundRelationName returns the name of the relation not found.
func (err ErrRelationNotFound) NotFoundRelationName() string {
	return err.relationName
}

func (err ErrRelationNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrRelationNotFound) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.namespaceName,
		"relation_or_permission_name": err.relationName,
	}
}

// NewRelationNotFoundErr constructs a new relation not found error.
func NewRelationNotFoundErr(nsName string, relationName string) error {
	return ErrRelationNotFound{
		error:         fmt.Errorf("relation/permission `%s` not found under definition `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

var _ sharederrors.UnknownRelationError = ErrRelationNotFound{}

// ErrRelationMissingTypeInfo defines an error for when type information is missing from a relation
// during a lookup.
type ErrRelationMissingTypeInfo struct {
	error
	namespaceName string
	relationName  string
}

// NamespaceName returns the name of the namespace in which the relation was found.
func (err ErrRelationMissingTypeInfo) NamespaceName() string {
	return err.namespaceName
}

// RelationName returns the name of the relation missing type information.
func (err ErrRelationMissingTypeInfo) RelationName() string {
	return err.relationName
}

func (err ErrRelationMissingTypeInfo) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrRelationMissingTypeInfo) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
		"relation_name":   err.relationName,
	}
}

// NewRelationMissingTypeInfoErr constructs a new relation not missing type information error.
func NewRelationMissingTypeInfoErr(nsName string, relationName string) error {
	return ErrRelationMissingTypeInfo{
		error:         fmt.Errorf("relation/permission `%s` under definition `%s` is missing type information", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

// ErrInvalidArgument occurs when a request sent has an invalid argument.
type ErrInvalidArgument struct {
	error
}

// NewErrInvalidArgument constructs a request sent has an invalid argument.
func NewErrInvalidArgument(baseErr error) error {
	return ErrInvalidArgument{
		error: baseErr,
	}
}

// ErrUnimplemented is returned when some functionality is not yet supported.
type ErrUnimplemented struct {
	error
}

// NewUnimplementedErr constructs a new unimplemented error.
func NewUnimplementedErr(baseErr error) error {
	return ErrUnimplemented{
		error: baseErr,
	}
}

// ErrInvalidCursor is returned when a cursor is no longer valid.
type ErrInvalidCursor struct {
	error
}

// NewInvalidCursorErr constructs a new unimplemented error.
func NewInvalidCursorErr(dispatchCursorVersion uint32, cursor *dispatch.Cursor) error {
	return ErrInvalidCursor{
		error: fmt.Errorf("the supplied cursor is no longer valid: found version %d, expected version %d", cursor.DispatchVersion, dispatchCursorVersion),
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err ErrInvalidCursor) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_CURSOR,
			map[string]string{
				"details": "cursor was used against an incompatible version of SpiceDB",
			},
		),
	)
}
