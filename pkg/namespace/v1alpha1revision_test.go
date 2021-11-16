package namespace

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestDeterminism(t *testing.T) {
	revisions := map[string]decimal.Decimal{
		"foo":  decimal.NewFromInt(1),
		"bar":  decimal.NewFromInt(2),
		"baz":  decimal.NewFromInt(3),
		"meh":  decimal.NewFromInt(4),
		"yarg": decimal.NewFromInt(5),
	}

	computed1, err := ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	computed2, err := ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	require.Equal(t, computed1, computed2)
}

func TestEncodeDecode(t *testing.T) {
	revisions := map[string]decimal.Decimal{
		"foo":  decimal.NewFromInt(1),
		"bar":  decimal.NewFromInt(2),
		"baz":  decimal.NewFromInt(3),
		"meh":  decimal.NewFromInt(4),
		"yarg": decimal.NewFromInt(5),
	}

	computed, err := ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	decoded, err := DecodeV1Alpha1Revision(computed)
	require.NoError(t, err)

	require.Equal(t, revisions, decoded)

	different := map[string]decimal.Decimal{
		"foo": decimal.NewFromInt(2),
		"bar": decimal.NewFromInt(7),
	}

	computedDiffernt, err := ComputeV1Alpha1Revision(different)
	require.NoError(t, err)

	decodedDifferent, err := DecodeV1Alpha1Revision(computedDiffernt)
	require.NoError(t, err)

	require.Equal(t, different, decodedDifferent)
	require.NotEqual(t, revisions, decodedDifferent)
}

func TestInvalidDecode(t *testing.T) {
	_, err := DecodeV1Alpha1Revision("blahblah")
	require.Error(t, err)
}

func TestEncodeInObject(t *testing.T) {
	revisions := map[string]decimal.Decimal{
		"foo":  decimal.NewFromInt(1),
		"bar":  decimal.NewFromInt(2),
		"baz":  decimal.NewFromInt(3),
		"meh":  decimal.NewFromInt(4),
		"yarg": decimal.NewFromInt(5),
	}

	// Encode the revision.
	computed, err := ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	// Ensure it can be used as an object ID in the API.
	objRef := &v1.ObjectReference{
		ObjectType: "someobjecttype",
		ObjectId:   computed,
	}
	require.NoError(t, objRef.Validate())
}
