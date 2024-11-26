package namespace

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/sharederrors"
)

// NamespaceNotFoundError occurs when a namespace was not found.
type NamespaceNotFoundError struct {
	error
	namespaceName string
}

// NotFoundNamespaceName is the name of the namespace not found.
func (err NamespaceNotFoundError) NotFoundNamespaceName() string {
	return err.namespaceName
}

// MarshalZerologObject implements zerolog object marshalling.
func (err NamespaceNotFoundError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err NamespaceNotFoundError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
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

// DuplicateRelationError occurs when a duplicate relation was found inside a namespace.
type DuplicateRelationError struct {
	error
	namespaceName string
	relationName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err DuplicateRelationError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err DuplicateRelationError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.namespaceName,
		"relation_or_permission_name": err.relationName,
	}
}

// PermissionsCycleError occurs when a cycle exists within permissions.
type PermissionsCycleError struct {
	error
	namespaceName   string
	permissionNames []string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err PermissionsCycleError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("permissions", strings.Join(err.permissionNames, ", "))
}

// DetailsMetadata returns the metadata for details for this error.
func (err PermissionsCycleError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":  err.namespaceName,
		"permission_names": strings.Join(err.permissionNames, ","),
	}
}

// UnusedCaveatParameterError indicates that a caveat parameter is unused in the caveat expression.
type UnusedCaveatParameterError struct {
	error
	caveatName string
	paramName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err UnusedCaveatParameterError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("caveat", err.caveatName).Str("param", err.paramName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err UnusedCaveatParameterError) DetailsMetadata() map[string]string {
	return map[string]string{
		"caveat_name":    err.caveatName,
		"parameter_name": err.paramName,
	}
}

// NewNamespaceNotFoundErr constructs a new namespace not found error.
func NewNamespaceNotFoundErr(nsName string) error {
	return NamespaceNotFoundError{
		error:         fmt.Errorf("object definition `%s` not found", nsName),
		namespaceName: nsName,
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

// NewDuplicateRelationError constructs an error indicating that a relation was defined more than once in a namespace.
func NewDuplicateRelationError(nsName string, relationName string) error {
	return DuplicateRelationError{
		error:         fmt.Errorf("found duplicate relation/permission name `%s` under definition `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

// NewPermissionsCycleErr constructs an error indicating that a cycle exists amongst permissions.
func NewPermissionsCycleErr(nsName string, permissionNames []string) error {
	return PermissionsCycleError{
		error:           fmt.Errorf("under definition `%s`, there exists a cycle in permissions: %s", nsName, strings.Join(permissionNames, ", ")),
		namespaceName:   nsName,
		permissionNames: permissionNames,
	}
}

// NewUnusedCaveatParameterErr constructs indicating that a parameter was unused in a caveat expression.
func NewUnusedCaveatParameterErr(caveatName string, paramName string) error {
	return UnusedCaveatParameterError{
		error:      fmt.Errorf("parameter `%s` for caveat `%s` is unused", paramName, caveatName),
		caveatName: caveatName,
		paramName:  paramName,
	}
}

var (
	_ sharederrors.UnknownNamespaceError = NamespaceNotFoundError{}
	_ sharederrors.UnknownRelationError  = RelationNotFoundError{}
)
