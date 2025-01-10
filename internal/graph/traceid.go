package graph

import (
	"github.com/google/uuid"
)

// NewTraceID generates a new trace ID. The trace IDs will only be unique with
// a single dispatch request tree and should not be used for any other purpose.
// This function currently uses the UUID library to generate a new trace ID,
// which means it should not be invoked from performance-critical code paths.
func NewTraceID() string {
	return uuid.NewString()
}
