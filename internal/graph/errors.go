package graph

import (
	"errors"
	"fmt"
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
