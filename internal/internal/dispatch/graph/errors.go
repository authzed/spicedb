package graph

import (
	"fmt"

	"github.com/rs/zerolog"
)

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

// NotFoundNamespaceName returns the name of the namespace that was not found.
func (enf ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return enf.namespaceName
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (enf ErrNamespaceNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", enf.Error()).Str("namespace", enf.namespaceName)
}

// NewNamespaceNotFoundErr constructs a new namespace not found error.
func NewNamespaceNotFoundErr(nsName string) error {
	return ErrNamespaceNotFound{
		error:         fmt.Errorf("object definition `%s` not found", nsName),
		namespaceName: nsName,
	}
}

// ErrRelationNotFound occurs when a relation was not found under a namespace.
type ErrRelationNotFound struct {
	error
	namespaceName string
	relationName  string
}

// NamespaceName returns the name of the namespace in which the relation was not found.
func (erf ErrRelationNotFound) NamespaceName() string {
	return erf.namespaceName
}

// NotFoundRelationName returns the name of the relation not found.
func (erf ErrRelationNotFound) NotFoundRelationName() string {
	return erf.relationName
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (erf ErrRelationNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", erf.Error()).Str("namespace", erf.namespaceName).Str("relation", erf.relationName)
}

// NewRelationNotFoundErr constructs a new relation not found error.
func NewRelationNotFoundErr(nsName string, relationName string) error {
	return ErrRelationNotFound{
		error:         fmt.Errorf("relation/permission `%s` not found under definition `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}
