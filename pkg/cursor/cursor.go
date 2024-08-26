package cursor

import (
	"encoding/base64"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Encode converts a decoded cursor to its opaque version.
func Encode(decoded *impl.DecodedCursor) (*v1.Cursor, error) {
	marshalled, err := decoded.MarshalVT()
	if err != nil {
		return nil, NewInvalidCursorErr(fmt.Errorf(errEncodeError, err))
	}

	return &v1.Cursor{
		Token: base64.StdEncoding.EncodeToString(marshalled),
	}, nil
}

// Decode converts an encoded cursor to its decoded version.
func Decode(encoded *v1.Cursor) (*impl.DecodedCursor, error) {
	if encoded == nil {
		return nil, NewInvalidCursorErr(errors.New("cursor pointer was nil"))
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(encoded.Token)
	if err != nil {
		return nil, NewInvalidCursorErr(fmt.Errorf(errDecodeError, err))
	}

	decoded := &impl.DecodedCursor{}
	if err := decoded.UnmarshalVT(decodedBytes); err != nil {
		return nil, NewInvalidCursorErr(fmt.Errorf(errDecodeError, err))
	}

	return decoded, nil
}

// EncodeFromDispatchCursor encodes an internal dispatching cursor into a V1 cursor for external
// consumption, including the provided call context to ensure the API cursor reflects the calling
// API method. The call hash should contain all the parameters of the calling API function,
// as well as its revision and name.
func EncodeFromDispatchCursor(dispatchCursor *dispatch.Cursor, callAndParameterHash string, revision datastore.Revision, flags map[string]string) (*v1.Cursor, error) {
	if dispatchCursor == nil {
		return nil, spiceerrors.MustBugf("got nil dispatch cursor")
	}

	return Encode(&impl.DecodedCursor{
		VersionOneof: &impl.DecodedCursor_V1{
			V1: &impl.V1Cursor{
				Revision:              revision.String(),
				DispatchVersion:       dispatchCursor.DispatchVersion,
				Sections:              dispatchCursor.Sections,
				CallAndParametersHash: callAndParameterHash,
				Flags:                 flags,
			},
		},
	})
}

// GetCursorFlag retrieves a flag from an encoded API cursor, if any.
func GetCursorFlag(encoded *v1.Cursor, flagName string) (string, bool, error) {
	decoded, err := Decode(encoded)
	if err != nil {
		return "", false, err
	}

	v1decoded := decoded.GetV1()
	if v1decoded == nil {
		return "", false, NewInvalidCursorErr(ErrNilCursor)
	}

	value, ok := v1decoded.Flags[flagName]
	return value, ok, nil
}

// DecodeToDispatchCursor decodes an encoded API cursor into an internal dispatching cursor,
// ensuring that the provided call context matches that encoded into the API cursor. The call
// hash should contain all the parameters of the calling API function, as well as its revision
// and name.
func DecodeToDispatchCursor(encoded *v1.Cursor, callAndParameterHash string) (*dispatch.Cursor, map[string]string, error) {
	decoded, err := Decode(encoded)
	if err != nil {
		return nil, nil, err
	}

	v1decoded := decoded.GetV1()
	if v1decoded == nil {
		return nil, nil, NewInvalidCursorErr(ErrNilCursor)
	}

	if v1decoded.CallAndParametersHash != callAndParameterHash {
		return nil, nil, NewInvalidCursorErr(ErrHashMismatch)
	}

	return &dispatch.Cursor{
		DispatchVersion: v1decoded.DispatchVersion,
		Sections:        v1decoded.Sections,
	}, v1decoded.Flags, nil
}

// DecodeToDispatchRevision decodes an encoded API cursor into an internal dispatch revision.
// NOTE: this method does *not* verify the caller's method signature.
func DecodeToDispatchRevision(encoded *v1.Cursor, ds revisionDecoder) (datastore.Revision, error) {
	decoded, err := Decode(encoded)
	if err != nil {
		return nil, err
	}

	v1decoded := decoded.GetV1()
	if v1decoded == nil {
		return nil, ErrNilCursor
	}

	parsed, err := ds.RevisionFromString(v1decoded.Revision)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errDecodeError, err)
	}

	return parsed, nil
}

type revisionDecoder interface {
	RevisionFromString(string) (datastore.Revision, error)
}
