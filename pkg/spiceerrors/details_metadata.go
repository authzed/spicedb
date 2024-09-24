package spiceerrors

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
)

// MetadataKey is the type used to represent the keys of the metadata map in
// the error details.
type MetadataKey string

// DebugTraceErrorDetailsKey is the key used to store the debug trace in the error details.
// The value is expected to be a string containing the proto text of a DebugInformation message.
const DebugTraceErrorDetailsKey MetadataKey = "debug_trace_proto_text"

// AppendDetailsMetadata appends the key-value pair to the error details metadata.
// If the error is nil or is not a status error, it is returned as is.
func AppendDetailsMetadata(err error, key MetadataKey, value string) error {
	if err == nil {
		return nil
	}

	s, ok := status.FromError(err)
	if !ok {
		return err
	}

	var foundErrDetails *errdetails.ErrorInfo
	var otherDetails []protoiface.MessageV1
	for _, details := range s.Details() {
		if errInfo, ok := details.(*errdetails.ErrorInfo); ok {
			foundErrDetails = errInfo
		} else if cast, ok := details.(protoiface.MessageV1); ok {
			otherDetails = append(otherDetails, cast)
		}
	}

	if foundErrDetails != nil {
		if foundErrDetails.Metadata == nil {
			foundErrDetails.Metadata = make(map[string]string, 1)
		}

		foundErrDetails.Metadata[string(key)] = value
	}

	return WithCodeAndDetailsAsError(
		fmt.Errorf("%s", s.Message()),
		s.Code(),
		append(otherDetails, foundErrDetails)...,
	)
}

// WithReplacedDetails replaces the details of the error with the provided details.
// If the error is nil or is not a status error, it is returned as is.
// If the error does not have the details to replace, the provided details are appended.
func WithReplacedDetails[T protoiface.MessageV1](err error, toReplace T) error {
	if err == nil {
		return nil
	}

	s, ok := status.FromError(err)
	if !ok {
		return err
	}

	details := make([]protoiface.MessageV1, 0, len(s.Details()))
	wasReplaced := false
	for _, current := range s.Details() {
		if _, ok := current.(T); ok {
			details = append(details, toReplace)
			wasReplaced = true
			continue
		}

		if cast, ok := current.(protoiface.MessageV1); ok {
			details = append(details, cast)
		}
	}

	if !wasReplaced {
		details = append(details, toReplace)
	}

	return WithCodeAndDetailsAsError(
		fmt.Errorf("%s", s.Message()),
		s.Code(),
		details...,
	)
}

// GetDetails returns the details of the error if they are of the provided type, if any.
func GetDetails[T protoiface.MessageV1](err error) (T, bool) {
	if err == nil {
		return *new(T), false
	}

	s, ok := status.FromError(err)
	if !ok {
		return *new(T), false
	}

	for _, details := range s.Details() {
		if cast, ok := details.(T); ok {
			return cast, true
		}
	}

	return *new(T), false
}
