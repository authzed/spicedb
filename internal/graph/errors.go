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

// CheckFailureError occurs when check failed in some manner. Note this should not apply to
// namespaces and relations not being found.
type CheckFailureError struct {
	error
}

func (e CheckFailureError) Unwrap() error {
	return e.error
}

// NewCheckFailureErr constructs a new check failed error.
func NewCheckFailureErr(baseErr error) error {
	return CheckFailureError{
		error: fmt.Errorf("error performing check: %w", baseErr),
	}
}

// ExpansionFailureError occurs when expansion failed in some manner. Note this should not apply to
// namespaces and relations not being found.
type ExpansionFailureError struct {
	error
}

func (e ExpansionFailureError) Unwrap() error {
	return e.error
}

// NewExpansionFailureErr constructs a new expansion failed error.
func NewExpansionFailureErr(baseErr error) error {
	return ExpansionFailureError{
		error: fmt.Errorf("error performing expand: %w", baseErr),
	}
}

// AlwaysFailError is returned when an internal error leads to an operation
// guaranteed to fail.
type AlwaysFailError struct {
	error
}

// NewAlwaysFailErr constructs a new always fail error.
func NewAlwaysFailErr() error {
	return AlwaysFailError{
		error: errors.New("always fail"),
	}
}

// RelationNotFoundError occurs when a relation was not found under a namespace.
type RelationNotFoundError struct {
	error
	namespaceName string
	relationName  string
}

// NamespaceName returns the name of the namespace in which the relation was not found.
func (err RelationNotFoundError) NamespaceName() string {
	return err.namespaceName
}

// NotFoundRelationName returns the name of the relation not found.
func (err RelationNotFoundError) NotFoundRelationName() string {
	return err.relationName
}

func (err RelationNotFoundError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err RelationNotFoundError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.namespaceName,
		"relation_or_permission_name": err.relationName,
	}
}

// NewRelationNotFoundErr constructs a new relation not found error.
func NewRelationNotFoundErr(nsName string, relationName string) error {
	return RelationNotFoundError{
		error:         fmt.Errorf("relation/permission `%s` not found under definition `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

var _ sharederrors.UnknownRelationError = RelationNotFoundError{}

// RelationMissingTypeInfoError defines an error for when type information is missing from a relation
// during a lookup.
type RelationMissingTypeInfoError struct {
	error
	namespaceName string
	relationName  string
}

// NamespaceName returns the name of the namespace in which the relation was found.
func (err RelationMissingTypeInfoError) NamespaceName() string {
	return err.namespaceName
}

// RelationName returns the name of the relation missing type information.
func (err RelationMissingTypeInfoError) RelationName() string {
	return err.relationName
}

func (err RelationMissingTypeInfoError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err RelationMissingTypeInfoError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
		"relation_name":   err.relationName,
	}
}

// NewRelationMissingTypeInfoErr constructs a new relation not missing type information error.
func NewRelationMissingTypeInfoErr(nsName string, relationName string) error {
	return RelationMissingTypeInfoError{
		error:         fmt.Errorf("relation/permission `%s` under definition `%s` is missing type information", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

// WildcardNotAllowedError occurs when a request sent has an invalid wildcard argument.
type WildcardNotAllowedError struct {
	error

	fieldName string
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err WildcardNotAllowedError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_WILDCARD_NOT_ALLOWED,
			map[string]string{
				"field": err.fieldName,
			},
		),
	)
}

// NewWildcardNotAllowedErr constructs an error indicating that a wildcard was not allowed.
func NewWildcardNotAllowedErr(message string, fieldName string) error {
	return WildcardNotAllowedError{
		error:     fmt.Errorf("invalid argument: %s", message),
		fieldName: fieldName,
	}
}

// UnimplementedError is returned when some functionality is not yet supported.
type UnimplementedError struct {
	error
}

// NewUnimplementedErr constructs a new unimplemented error.
func NewUnimplementedErr(baseErr error) error {
	return UnimplementedError{
		error: baseErr,
	}
}

func (e UnimplementedError) Unwrap() error {
	return e.error
}

// InvalidCursorError is returned when a cursor is no longer valid.
type InvalidCursorError struct {
	error
}

// NewInvalidCursorErr constructs a new unimplemented error.
func NewInvalidCursorErr(dispatchCursorVersion uint32, cursor *dispatch.Cursor) error {
	return InvalidCursorError{
		error: fmt.Errorf("the supplied cursor is no longer valid: found version %d, expected version %d", cursor.DispatchVersion, dispatchCursorVersion),
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err InvalidCursorError) GRPCStatus() *status.Status {
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
