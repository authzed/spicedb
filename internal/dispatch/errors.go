package dispatch

import (
	"fmt"
)

// MaxDepthExceededError is an error returned when the maximum depth for dispatching has been exceeded.
type MaxDepthExceededError struct {
	error

	// Request is the request that exceeded the maximum depth.
	Request DispatchableRequest
}

// NewMaxDepthExceededError creates a new MaxDepthExceededError.
func NewMaxDepthExceededError(req DispatchableRequest) error {
	return MaxDepthExceededError{
		fmt.Errorf("max depth exceeded: this usually indicates a recursive or too deep data dependency"),
		req,
	}
}
