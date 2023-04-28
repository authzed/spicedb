package cursor

import (
	"encoding/base64"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
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

// Encode converts a decoded cursor to its opaque version.
func Encode(decoded *impl.DecodedCursor) (*v1.Cursor, error) {
	marshalled, err := decoded.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf(errEncodeError, err)
	}
	return &v1.Cursor{
		Token: base64.StdEncoding.EncodeToString(marshalled),
	}, nil
}

// Decode converts an encoded cursor to its decoded version.
func Decode(encoded *v1.Cursor) (*impl.DecodedCursor, error) {
	if encoded == nil {
		return nil, fmt.Errorf(errDecodeError, ErrNilCursor)
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(encoded.Token)
	if err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}
	decoded := &impl.DecodedCursor{}
	if err := decoded.UnmarshalVT(decodedBytes); err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}
	return decoded, nil
}

// EncodeFromDispatchCursor encodes an internal dispatching cursor into a V1 cursor for external
// consumption, including the provided call context to ensure the API cursor reflects the calling
// API method. The call hash should contain all the parameters of the calling API function,
// as well as its revision and name.
func EncodeFromDispatchCursor(dispatchCursor *dispatch.Cursor, callAndParameterHash string) (*v1.Cursor, error) {
	return Encode(&impl.DecodedCursor{
		VersionOneof: &impl.DecodedCursor_V1{
			V1: &impl.V1Cursor{
				Sections:              dispatchCursor.Sections,
				CallAndParametersHash: callAndParameterHash,
			},
		},
	})
}

// DecodeToDispatchCursor decodes an encoded API cursor into an internal dispatching cursor,
// ensuring that the provided call context matches that encoded into the API cursor. The call
// hash should contain all the parameters of the calling API function, as well as its revision
// and name.
func DecodeToDispatchCursor(encoded *v1.Cursor, callAndParameterHash string) (*dispatch.Cursor, error) {
	decoded, err := Decode(encoded)
	if err != nil {
		return nil, err
	}

	v1decoded := decoded.GetV1()
	if v1decoded == nil {
		return nil, ErrNilCursor
	}

	if v1decoded.CallAndParametersHash != callAndParameterHash {
		return nil, ErrHashMismatch
	}

	return &dispatch.Cursor{
		Sections: v1decoded.Sections,
	}, nil
}
