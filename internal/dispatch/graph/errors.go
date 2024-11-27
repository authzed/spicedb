package graph

import (
	"fmt"

	"github.com/rs/zerolog"
)

// NamespaceNotFoundError occurs when a namespace was not found.
type NamespaceNotFoundError struct {
	error
	namespaceName string
}

// NotFoundNamespaceName returns the name of the namespace that was not found.
func (err NamespaceNotFoundError) NotFoundNamespaceName() string {
	return err.namespaceName
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err NamespaceNotFoundError) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err NamespaceNotFoundError) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
	}
}

// NewNamespaceNotFoundErr constructs a new namespace not found error.
func NewNamespaceNotFoundErr(nsName string) error {
	return NamespaceNotFoundError{
		error:         fmt.Errorf("object definition `%s` not found", nsName),
		namespaceName: nsName,
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

// MarshalZerologObject implements zerolog.LogObjectMarshaler
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
