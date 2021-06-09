// Package zookie converts integers to zookies and vice versa
package zookie

import (
	"encoding/base64"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
	zookie "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/impl"
	"github.com/shopspring/decimal"
)

// Public facing errors
const (
	errEncodeError = "error encoding zookie: %w"
	errDecodeError = "error decoding zookie: %w"
)

var (
	// ErrNilZookie is returned as the base error when nil is provided as the
	// zookie argument to Decode
	ErrNilZookie = errors.New("zookie pointer was nil")
)

// NewFromRevision generates an encoded zookie from an integral revision.
func NewFromRevision(revision decimal.Decimal) *pb.Zookie {
	toEncode := &zookie.DecodedZookie{
		Version: 2,
		VersionOneof: &zookie.DecodedZookie_V2{
			V2: &zookie.DecodedZookie_V2Zookie{
				Revision: revision.String(),
			},
		},
	}
	encoded, err := Encode(toEncode)
	if err != nil {
		// All uint64s should be inherently encodeable
		panic(fmt.Errorf(errEncodeError, err))
	}

	return encoded
}

// Encode converts a decoded zookie to its opaque version.
func Encode(decoded *zookie.DecodedZookie) (*pb.Zookie, error) {
	marshalled, err := proto.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf(errEncodeError, err)
	}
	return &pb.Zookie{
		Token: base64.StdEncoding.EncodeToString(marshalled),
	}, nil
}

// Decode converts an encoded zookie to its decoded version.
func Decode(encoded *pb.Zookie) (*zookie.DecodedZookie, error) {
	if encoded == nil {
		return nil, fmt.Errorf(errDecodeError, ErrNilZookie)
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(encoded.Token)
	if err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}
	decoded := &zookie.DecodedZookie{}
	if err := proto.Unmarshal(decodedBytes, decoded); err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}
	return decoded, nil
}

func DecodeRevision(encoded *pb.Zookie) (decimal.Decimal, error) {
	decoded, err := Decode(encoded)
	if err != nil {
		return decimal.Zero, err
	}

	switch decoded.Version {
	case 1:
		return decimal.NewFromInt(int64(decoded.GetV1().Revision)), nil
	case 2:
		parsed, err := decimal.NewFromString(decoded.GetV2().Revision)
		if err != nil {
			return decimal.Zero, fmt.Errorf(errDecodeError, err)
		}
		return parsed, nil
	default:
		return decimal.Zero, fmt.Errorf(errDecodeError, fmt.Errorf("unknown zookie version: %d", decoded.Version))
	}
}
