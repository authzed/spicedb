package graph

import (
	"errors"
	"fmt"
)

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

func (enf ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return enf.namespaceName
}

func NewNamespaceNotFoundErr(nsName string) error {
	return ErrNamespaceNotFound{
		error:         fmt.Errorf("namespace `%s` not found", nsName),
		namespaceName: nsName,
	}
}

// ErrRelationNotFound occurs when a relation was not found under a namespace.
type ErrRelationNotFound struct {
	error
	namespaceName string
	relationName  string
}

func (erf ErrRelationNotFound) NamespaceName() string {
	return erf.namespaceName
}

func (erf ErrRelationNotFound) NotFoundRelationName() string {
	return erf.relationName
}

func NewRelationNotFoundErr(nsName string, relationName string) error {
	return ErrRelationNotFound{
		error:         fmt.Errorf("relation/permission `%s` not found under namespace `%s`", relationName, nsName),
		namespaceName: nsName,
		relationName:  relationName,
	}
}

// ErrRequestCanceled occurs when a request has been canceled.
type ErrRequestCanceled struct {
	error
}

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

func NewAlwaysFailErr() error {
	return ErrAlwaysFail{
		error: errors.New("always fail"),
	}
}
