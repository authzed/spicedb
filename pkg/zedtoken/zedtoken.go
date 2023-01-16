// Package zedtoken converts decimal.Decimal to zedtoken and vice versa
package zedtoken

import (
	"encoding/base64"
	"errors"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	zedtoken "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

// Public facing errors
const (
	errEncodeError = "error encoding zedtoken: %w"
	errDecodeError = "error decoding zedtoken: %w"
)

// ErrNilZedToken is returned as the base error when nil is provided as the
// zedtoken argument to Decode
var ErrNilZedToken = errors.New("zedtoken pointer was nil")

// MustNewFromRevision generates an encoded zedtoken from an integral revision.
func MustNewFromRevision(revision datastore.Revision) *v1.ZedToken {
	encoded, err := NewFromRevision(revision)
	if err != nil {
		panic(err)
	}
	return encoded
}

// NewFromRevision generates an encoded zedtoken from an integral revision.
func NewFromRevision(revision datastore.Revision) (*v1.ZedToken, error) {
	toEncode := &zedtoken.DecodedZedToken{
		VersionOneof: &zedtoken.DecodedZedToken_V1{
			V1: &zedtoken.DecodedZedToken_V1ZedToken{
				Revision: revision.String(),
			},
		},
	}
	encoded, err := Encode(toEncode)
	if err != nil {
		return nil, fmt.Errorf(errEncodeError, err)
	}

	return encoded, nil
}

// Encode converts a decoded zedtoken to its opaque version.
func Encode(decoded *zedtoken.DecodedZedToken) (*v1.ZedToken, error) {
	marshalled, err := decoded.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf(errEncodeError, err)
	}
	return &v1.ZedToken{
		Token: base64.StdEncoding.EncodeToString(marshalled),
	}, nil
}

// Decode converts an encoded zedtoken to its decoded version.
func Decode(encoded *v1.ZedToken) (*zedtoken.DecodedZedToken, error) {
	if encoded == nil {
		return nil, fmt.Errorf(errDecodeError, ErrNilZedToken)
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(encoded.Token)
	if err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}
	decoded := &zedtoken.DecodedZedToken{}
	if err := decoded.UnmarshalVT(decodedBytes); err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}
	return decoded, nil
}

// DecodeRevision converts and extracts the revision from a zedtoken or legacy zookie.
func DecodeRevision(encoded *v1.ZedToken, ds revisionDecoder) (datastore.Revision, error) {
	decoded, err := Decode(encoded)
	if err != nil {
		return datastore.NoRevision, err
	}

	switch ver := decoded.VersionOneof.(type) {
	case *zedtoken.DecodedZedToken_DeprecatedV1Zookie:
		return revision.NewFromDecimal(decimal.NewFromInt(int64(ver.DeprecatedV1Zookie.Revision))), nil
	case *zedtoken.DecodedZedToken_V1:
		parsed, err := ds.RevisionFromString(ver.V1.Revision)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errDecodeError, err)
		}
		return parsed, nil
	default:
		return datastore.NoRevision, fmt.Errorf(errDecodeError, fmt.Errorf("unknown zookie version: %T", decoded.VersionOneof))
	}
}

type revisionDecoder interface {
	RevisionFromString(string) (datastore.Revision, error)
}
