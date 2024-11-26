package memdb

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// SerializationMaxRetriesReachedError occurs when a write request has reached its maximum number
// of retries due to serialization errors.
type SerializationMaxRetriesReachedError struct {
	error
}

// NewSerializationMaxRetriesReachedErr constructs a new max retries reached error.
func NewSerializationMaxRetriesReachedErr(baseErr error) error {
	return SerializationMaxRetriesReachedError{
		error: baseErr,
	}
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err SerializationMaxRetriesReachedError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.DeadlineExceeded,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_UNSPECIFIED,
			map[string]string{
				"details": "too many updates were made to the in-memory datastore at once; this datastore has limited write throughput capability",
			},
		),
	)
}
