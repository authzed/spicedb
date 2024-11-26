package cursor

import (
	"errors"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Public facing errors
const (
	errEncodeError = "error encoding cursor: %w"
	errDecodeError = "error decoding cursor: %w"
)

// ErrNilCursor is returned as the base error when nil is provided as the
// cursor argument to Decode
var ErrNilCursor = errors.New("cursor pointer was nil")

// ErrHashMismatch is returned as the base error when a mismatching hash was given to the decoder.
var ErrHashMismatch = errors.New("the cursor provided does not have the same arguments as the original API call; please ensure you are making the same API call, with the exact same parameters (besides the cursor)")

// InvalidCursorError occurs when a cursor could not be decoded.
type InvalidCursorError struct {
	error
}

// GRPCStatus implements retrieving the gRPC status for the error.
func (err InvalidCursorError) GRPCStatus() *status.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.InvalidArgument,
		spiceerrors.ForReason(
			v1.ErrorReason_ERROR_REASON_INVALID_CURSOR,
			nil,
		),
	)
}

// NewInvalidCursorErr creates and returns a new invalid cursor error.
func NewInvalidCursorErr(err error) error {
	return InvalidCursorError{err}
}
