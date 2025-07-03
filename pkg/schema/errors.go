package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/sharederrors"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// DefinitionNotFoundError occurs when a definition was not found.
type DefinitionNotFoundError struct {
	error
	definitionName string
}

// NotFoundNamespaceName is the name of the definition not found.
func (err DefinitionNotFoundError) NotFoundNamespaceName() string {
	return err.definitionName
}

// MarshalZerologObject implements zerolog object marshalling.
func (err DefinitionNotFoundError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err DefinitionNotFoundError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.definitionName,
	}
}

// RelationNotFoundError occurs when a relation was not found under a definition.
type RelationNotFoundError struct {
	error
	definitionName string
	relationName   string
}

// NamespaceName returns the name of the definition in which the relation was not found.
func (err RelationNotFoundError) NamespaceName() string {
	return err.definitionName
}

// NotFoundRelationName returns the name of the relation not found.
func (err RelationNotFoundError) NotFoundRelationName() string {
	return err.relationName
}

// MarshalZerologObject implements zerolog object marshalling.
func (err RelationNotFoundError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err RelationNotFoundError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.definitionName,
		"relation_or_permission_name": err.relationName,
	}
}

// CaveatNotFoundError occurs when a caveat was not found.
type CaveatNotFoundError struct {
	error
	caveatName string
}

// CaveatName returns the name of the caveat not found.
func (err CaveatNotFoundError) CaveatName() string {
	return err.caveatName
}

// MarshalZerologObject implements zerolog object marshalling.
func (err CaveatNotFoundError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("caveat", err.caveatName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err CaveatNotFoundError) DetailsMetadata() map[string]string {
	return map[string]string{
		"caveat_name": err.caveatName,
	}
}

// DuplicateRelationError occurs when a duplicate relation was found inside a definition.
type DuplicateRelationError struct {
	error
	definitionName string
	relationName   string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err DuplicateRelationError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err DuplicateRelationError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":             err.definitionName,
		"relation_or_permission_name": err.relationName,
	}
}

// PermissionUsedOnLeftOfArrowError occurs when a permission is used on the left side of an arrow
// expression.
type PermissionUsedOnLeftOfArrowError struct {
	error
	definitionName       string
	parentPermissionName string
	foundPermissionName  string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err PermissionUsedOnLeftOfArrowError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("permission", err.parentPermissionName).Str("usedPermission", err.foundPermissionName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err PermissionUsedOnLeftOfArrowError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":      err.definitionName,
		"permission_name":      err.parentPermissionName,
		"used_permission_name": err.foundPermissionName,
	}
}

// WildcardUsedInArrowError occurs when an arrow operates over a relation that contains a wildcard.
type WildcardUsedInArrowError struct {
	error
	definitionName       string
	parentPermissionName string
	accessedRelationName string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err WildcardUsedInArrowError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("parentPermissionName", err.parentPermissionName).Str("accessedRelationName", err.accessedRelationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err WildcardUsedInArrowError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":        err.definitionName,
		"permission_name":        err.parentPermissionName,
		"accessed_relation_name": err.accessedRelationName,
	}
}

// MissingAllowedRelationsError occurs when a relation is defined without any type information.
type MissingAllowedRelationsError struct {
	error
	definitionName string
	relationName   string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err MissingAllowedRelationsError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err MissingAllowedRelationsError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.definitionName,
		"relation_name":   err.relationName,
	}
}

// TransitiveWildcardError occurs when a wildcard relation in turn references another wildcard
// relation.
type TransitiveWildcardError struct {
	error
	definitionName string
	relationName   string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err TransitiveWildcardError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("relation", err.relationName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err TransitiveWildcardError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.definitionName,
		"relation_name":   err.relationName,
	}
}

// PermissionsCycleError occurs when a cycle exists within permissions.
type PermissionsCycleError struct {
	error
	definitionName  string
	permissionNames []string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err PermissionsCycleError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("permissions", strings.Join(err.permissionNames, ", "))
}

// DetailsMetadata returns the metadata for details for this error.
func (err PermissionsCycleError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":  err.definitionName,
		"permission_names": strings.Join(err.permissionNames, ","),
	}
}

// DuplicateAllowedRelationError indicates that an allowed relation was redefined on a relation.
type DuplicateAllowedRelationError struct {
	error
	definitionName        string
	relationName          string
	allowedRelationSource string
}

// MarshalZerologObject implements zerolog object marshalling.
func (err DuplicateAllowedRelationError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("definition", err.definitionName).Str("relation", err.relationName).Str("allowed-relation", err.allowedRelationSource)
}

// DetailsMetadata returns the metadata for details for this error.
func (err DuplicateAllowedRelationError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name":  err.definitionName,
		"relation_name":    err.relationName,
		"allowed_relation": err.allowedRelationSource,
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

// NewDefinitionNotFoundErr constructs a new definition not found error.
func NewDefinitionNotFoundErr(nsName string) error {
	return DefinitionNotFoundError{
		error:          fmt.Errorf("object definition `%s` not found", nsName),
		definitionName: nsName,
	}
}

// NewRelationNotFoundErr constructs a new relation not found error.
func NewRelationNotFoundErr(nsName string, relationName string) error {
	return RelationNotFoundError{
		error:          fmt.Errorf("relation/permission `%s` not found under definition `%s`", relationName, nsName),
		definitionName: nsName,
		relationName:   relationName,
	}
}

// NewCaveatNotFoundErr constructs a new caveat not found error.
func NewCaveatNotFoundErr(caveatName string) error {
	return CaveatNotFoundError{
		error:      fmt.Errorf("caveat `%s` not found", caveatName),
		caveatName: caveatName,
	}
}

// NewDuplicateRelationError constructs an error indicating that a relation was defined more than once in a definition.
func NewDuplicateRelationError(nsName string, relationName string) error {
	return DuplicateRelationError{
		error:          fmt.Errorf("found duplicate relation/permission name `%s` under definition `%s`", relationName, nsName),
		definitionName: nsName,
		relationName:   relationName,
	}
}

// NewDuplicateAllowedRelationErr constructs an error indicating that an allowed relation was defined more than once for a relation.
func NewDuplicateAllowedRelationErr(nsName string, relationName string, allowedRelationSource string) error {
	return DuplicateAllowedRelationError{
		error:                 fmt.Errorf("found duplicate allowed subject type `%s` on relation `%s` under definition `%s`", allowedRelationSource, relationName, nsName),
		definitionName:        nsName,
		relationName:          relationName,
		allowedRelationSource: allowedRelationSource,
	}
}

// NewPermissionUsedOnLeftOfArrowErr constructs an error indicating that a permission was used on the left side of an arrow.
func NewPermissionUsedOnLeftOfArrowErr(nsName string, parentPermissionName string, foundPermissionName string) error {
	return PermissionUsedOnLeftOfArrowError{
		error:                fmt.Errorf("under permission `%s` under definition `%s`: permissions cannot be used on the left hand side of an arrow (found `%s`)", parentPermissionName, nsName, foundPermissionName),
		definitionName:       nsName,
		parentPermissionName: parentPermissionName,
		foundPermissionName:  foundPermissionName,
	}
}

// NewWildcardUsedInArrowErr constructs an error indicating that an arrow operated over a relation with a wildcard type.
func NewWildcardUsedInArrowErr(nsName string, parentPermissionName string, foundRelationName string, wildcardTypeName string, wildcardRelationName string) error {
	return WildcardUsedInArrowError{
		error:                fmt.Errorf("for arrow under permission `%s`: relation `%s#%s` includes wildcard type `%s` via relation `%s`: wildcard relations cannot be used on the left side of arrows", parentPermissionName, nsName, foundRelationName, wildcardTypeName, wildcardRelationName),
		definitionName:       nsName,
		parentPermissionName: parentPermissionName,
		accessedRelationName: foundRelationName,
	}
}

// NewMissingAllowedRelationsErr constructs an error indicating that type information is missing for a relation.
func NewMissingAllowedRelationsErr(nsName string, relationName string) error {
	return MissingAllowedRelationsError{
		error:          fmt.Errorf("at least one allowed relation/permission is required to be defined for relation `%s`", relationName),
		definitionName: nsName,
		relationName:   relationName,
	}
}

// NewTransitiveWildcardErr constructs an error indicating that a transitive wildcard exists.
func NewTransitiveWildcardErr(nsName string, relationName string, foundRelationNamespace string, foundRelationName string, wildcardTypeName string, wildcardRelationReference string) error {
	return TransitiveWildcardError{
		error:          fmt.Errorf("for relation `%s`: relation/permission `%s#%s` includes wildcard type `%s` via relation `%s`: wildcard relations cannot be transitively included", relationName, foundRelationNamespace, foundRelationName, wildcardTypeName, wildcardRelationReference),
		definitionName: nsName,
		relationName:   relationName,
	}
}

// NewPermissionsCycleErr constructs an error indicating that a cycle exists amongst permissions.
func NewPermissionsCycleErr(nsName string, permissionNames []string) error {
	return PermissionsCycleError{
		error:           fmt.Errorf("under definition `%s`, there exists a cycle in permissions: %s", nsName, strings.Join(permissionNames, ", ")),
		definitionName:  nsName,
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
	_ sharederrors.UnknownNamespaceError = DefinitionNotFoundError{}
	_ sharederrors.UnknownRelationError  = RelationNotFoundError{}
)

// NewTypeWithSourceError creates a new type error at the specific position and with source code, wrapping the underlying
// error.
func NewTypeWithSourceError(wrapped error, withSource nspkg.WithSourcePosition, sourceCodeString string) error {
	sourcePosition := withSource.GetSourcePosition()
	if sourcePosition != nil {
		return asTypeError(spiceerrors.NewWithSourceError(
			wrapped,
			sourceCodeString,
			sourcePosition.ZeroIndexedLineNumber+1, // +1 to make 1-indexed
			sourcePosition.ZeroIndexedColumnPosition+1, // +1 to make 1-indexed
		))
	}

	return asTypeError(spiceerrors.NewWithSourceError(
		wrapped,
		sourceCodeString,
		0,
		0,
	))
}
