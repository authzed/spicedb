package namespace

import (
	"errors"
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
	e.Err(err).Str("namespace", err.namespaceName)
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
	e.Err(err).Str("namespace", err.namespaceName).Str("relation", err.relationName)
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
	e.Err(err).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrDuplicateRelation) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.namespaceName,
		"relation_or_permission_name": err.relationName,
	}
}

// ErrPermissionUsedOnLeftOfArrow occurs when a permission is used on the left side of an arrow
// expression.
type ErrPermissionUsedOnLeftOfArrow struct {
	error
	namespaceName        string
	parentPermissionName string
	foundPermissionName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrPermissionUsedOnLeftOfArrow) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err).Str("namespace", err.namespaceName).Str("permission", err.parentPermissionName).Str("usedPermission", err.foundPermissionName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrPermissionUsedOnLeftOfArrow) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":      err.namespaceName,
		"permission_name":      err.parentPermissionName,
		"used_permission_name": err.foundPermissionName,
	}
}

// ErrWildcardUsedInArrow occurs when an arrow operates over a relation that contains a wildcard.
type ErrWildcardUsedInArrow struct {
	error
	namespaceName        string
	parentPermissionName string
	accessedRelationName string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrWildcardUsedInArrow) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err).Str("namespace", err.namespaceName).Str("parentPermissionName", err.parentPermissionName).Str("accessedRelationName", err.accessedRelationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrWildcardUsedInArrow) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":        err.namespaceName,
		"permission_name":        err.parentPermissionName,
		"accessed_relation_name": err.accessedRelationName,
	}
}

// ErrMissingAllowedRelations occurs when a relation is defined without any type information.
type ErrMissingAllowedRelations struct {
	error
	namespaceName string
	relationName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrMissingAllowedRelations) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrMissingAllowedRelations) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
		"relation_name":   err.relationName,
	}
}

// ErrTransitiveWildcard occurs when a wildcard relation in turn references another wildcard
// relation.
type ErrTransitiveWildcard struct {
	error
	namespaceName string
	relationName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrTransitiveWildcard) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err).Str("namespace", err.namespaceName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrTransitiveWildcard) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
		"relation_name":   err.relationName,
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
	e.Err(err).Str("namespace", err.namespaceName).Str("permissions", strings.Join(err.permissionNames, ", "))
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrPermissionsCycle) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":  err.namespaceName,
		"permission_names": strings.Join(err.permissionNames, ","),
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

// NewPermissionUsedOnLeftOfArrowErr constructs an error indicating that a permission was used on the left side of an arrow.
func NewPermissionUsedOnLeftOfArrowErr(nsName string, parentPermissionName string, foundPermissionName string) error {
	return ErrPermissionUsedOnLeftOfArrow{
		error:                fmt.Errorf("under permission `%s` under definition `%s`: permissions cannot be used on the left hand side of an arrow (found `%s`)", parentPermissionName, nsName, foundPermissionName),
		namespaceName:        nsName,
		parentPermissionName: parentPermissionName,
		foundPermissionName:  foundPermissionName,
	}
}

// NewWildcardUsedInArrowErr constructs an error indicating that an arrow operated over a relation with a wildcard type.
func NewWildcardUsedInArrowErr(nsName string, parentPermissionName string, foundRelationName string, wildcardTypeName string, wildcardRelationName string) error {
	return ErrWildcardUsedInArrow{
		error:                fmt.Errorf("for arrow under permission `%s`: relation `%s#%s` includes wildcard type `%s` via relation `%s`: wildcard relations cannot be used on the left side of arrows", parentPermissionName, nsName, foundRelationName, wildcardTypeName, wildcardRelationName),
		namespaceName:        nsName,
		parentPermissionName: parentPermissionName,
		accessedRelationName: foundRelationName,
	}
}

// NewMissingAllowedRelationsErr constructs an error indicating that type information is missing for a relation.
func NewMissingAllowedRelationsErr(nsName string, relationName string) error {
	return ErrMissingAllowedRelations{
		error:         fmt.Errorf("at least one allowed relation/permission is required to be defined for relation `%s`", relationName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

// NewTransitiveWildcardErr constructs an error indicating that a transitive wildcard exists.
func NewTransitiveWildcardErr(nsName string, relationName string, foundRelationNamespace string, foundRelationName string, wildcardTypeName string, wildcardRelationReference string) error {
	return ErrTransitiveWildcard{
		error:         fmt.Errorf("for relation `%s`: relation/permission `%s#%s` includes wildcard type `%s` via relation `%s`: wildcard relations cannot be transitively included", relationName, foundRelationNamespace, foundRelationName, wildcardTypeName, wildcardRelationReference),
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

// asTypeError wraps another error in a type error.
func asTypeError(wrapped error) error {
	if wrapped == nil {
		return nil
	}

	var te TypeError
	if errors.As(wrapped, &te) {
		return wrapped
	}

	return TypeError{wrapped}
}

// TypeError wraps another error as a type error.
type TypeError struct {
	error
}

func (err TypeError) Unwrap() error {
	return err.error
}

var (
	_ sharederrors.UnknownNamespaceError = ErrNamespaceNotFound{}
	_ sharederrors.UnknownRelationError  = ErrRelationNotFound{}
)
