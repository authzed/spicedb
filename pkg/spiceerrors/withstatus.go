package spiceerrors

import (
	"errors"

	log "github.com/authzed/spicedb/internal/logging"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// Domain is the domain used for all errors.
const Domain = "authzed.com"

// WithCodeAndDetails returns a gRPC status message containing the error's message, the given
// status code and any supplied details.
func WithCodeAndDetails(err error, code codes.Code, details ...protoiface.MessageV1) *status.Status {
	created := status.New(code, err.Error())
	created, derr := created.WithDetails(details...)
	if derr != nil {
		log.Err(derr).Str("provided-error", err.Error()).Msg("could not add details to provided error")
	}
	return created
}

// WithCodeAndDetailsAsError returns an error containing the error's message, the given
// status code and any supplied details.
func WithCodeAndDetailsAsError(err error, code codes.Code, details ...protoiface.MessageV1) error {
	status := WithCodeAndDetails(err, code, details...)
	return withStatusError{err, status}
}

// ForReason returns an ErrorInfo block for a specific error reason as defined in the V1 API.
func ForReason(reason v1.ErrorReason, metadata map[string]string) *errdetails.ErrorInfo {
	return &errdetails.ErrorInfo{
		Reason:   v1.ErrorReason_name[int32(reason)],
		Domain:   Domain,
		Metadata: metadata,
	}
}

// WithCodeAndReason returns a new error which wraps the existing error with a gRPC code and
// a reason block.
func WithCodeAndReason(err error, code codes.Code, reason v1.ErrorReason) error {
	metadata := map[string]string{}

	var hasMetadata HasMetadata
	if ok := errors.As(err, &hasMetadata); ok {
		metadata = hasMetadata.DetailsMetadata()
	}

	status := WithCodeAndDetails(err, code, ForReason(reason, metadata))
	return withStatusError{err, status}
}

type SupportsAdditionalMetadata interface {
	WithAdditionalDetails(key MetadataKey, value string)
}

// WithAdditionalDetails adds an additional details field to the error if it is possible.
func WithAdditionalDetails(err error, key MetadataKey, value string) bool {
	var supportsAdditionalDetails SupportsAdditionalMetadata
	if ok := errors.As(err, &supportsAdditionalDetails); ok {
		supportsAdditionalDetails.WithAdditionalDetails(key, value)
		return true
	}

	return false
}

type withStatusError struct {
	error
	status *status.Status
}

func (err withStatusError) GRPCStatus() *status.Status {
	return err.status
}
