package graph

import (
	"errors"
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
