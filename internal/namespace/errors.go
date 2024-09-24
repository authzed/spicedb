package namespace

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/sharederrors"
)

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

// NotFoundNamespaceName is the name of the namespace not found.
func (err ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return err.namespaceName
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrNamespaceNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrNamespaceNotFound) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
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

// ErrDuplicateRelation occurs when a duplicate relation was found inside a namespace.
type ErrDuplicateRelation struct {
	error
	namespaceName string
	relationName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrDuplicateRelation) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrDuplicateRelation) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.namespaceName,
		"relation_or_permission_name": err.relationName,
	}
}

// ErrPermissionsCycle occurs when a cycle exists within permissions.
type ErrPermissionsCycle struct {
	error
	namespaceName   string
	permissionNames []string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrPermissionsCycle) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName).Str("permissions", strings.Join(err.permissionNames, ", "))
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrPermissionsCycle) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":  err.namespaceName,
		"permission_names": strings.Join(err.permissionNames, ","),
	}
}

// ErrUnusedCaveatParameter indicates that a caveat parameter is unused in the caveat expression.
type ErrUnusedCaveatParameter struct {
	error
	caveatName string
	paramName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrUnusedCaveatParameter) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("caveat", err.caveatName).Str("param", err.paramName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrUnusedCaveatParameter) DetailsMetadata() map[string]string {
	return map[string]string{
		"caveat_name":    err.caveatName,
		"parameter_name": err.paramName,
	}
}

// NewNamespaceNotFoundErr constructs a new namespace not found error.
func NewNamespaceNotFoundErr(nsName string) error {
	return ErrNamespaceNotFound{
		error:         fmt.Errorf("object definition `%s` not found", nsName),
		namespaceName: nsName,
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

// NewDuplicateRelationError constructs an error indicating that a relation was defined more than once in a namespace.
func NewDuplicateRelationError(nsName string, relationName string) error {
	return ErrDuplicateRelation{
		error:         fmt.Errorf("found duplicate relation/permission name `%s` under definition `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

// NewPermissionsCycleErr constructs an error indicating that a cycle exists amongst permissions.
func NewPermissionsCycleErr(nsName string, permissionNames []string) error {
	return ErrPermissionsCycle{
		error:           fmt.Errorf("under definition `%s`, there exists a cycle in permissions: %s", nsName, strings.Join(permissionNames, ", ")),
		namespaceName:   nsName,
		permissionNames: permissionNames,
	}
}

// NewUnusedCaveatParameterErr constructs indicating that a parameter was unused in a caveat expression.
func NewUnusedCaveatParameterErr(caveatName string, paramName string) error {
	return ErrUnusedCaveatParameter{
		error:      fmt.Errorf("parameter `%s` for caveat `%s` is unused", paramName, caveatName),
		caveatName: caveatName,
		paramName:  paramName,
	}
}

var (
	_ sharederrors.UnknownNamespaceError = ErrNamespaceNotFound{}
	_ sharederrors.UnknownRelationError  = ErrRelationNotFound{}
)
