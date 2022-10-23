package v1alpha1_test

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	rev "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/datastore"
)

var decoder revisions.DecimalDecoder

func TestDeterminism(t *testing.T) {
	revisions := map[string]datastore.Revision{
		"foo":  revisions.NewFromDecimal(decimal.NewFromInt(1)),
		"bar":  revisions.NewFromDecimal(decimal.NewFromInt(2)),
		"baz":  revisions.NewFromDecimal(decimal.NewFromInt(3)),
		"meh":  revisions.NewFromDecimal(decimal.NewFromInt(4)),
		"yarg": revisions.NewFromDecimal(decimal.NewFromInt(5)),
	}

	computed1, err := rev.ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	computed2, err := rev.ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	require.Equal(t, computed1, computed2)
}

func TestEncodeDecode(t *testing.T) {
	revs := map[string]datastore.Revision{
		"foo":  revisions.NewFromDecimal(decimal.NewFromInt(1)),
		"bar":  revisions.NewFromDecimal(decimal.NewFromInt(2)),
		"baz":  revisions.NewFromDecimal(decimal.NewFromInt(3)),
		"meh":  revisions.NewFromDecimal(decimal.NewFromInt(4)),
		"yarg": revisions.NewFromDecimal(decimal.NewFromInt(5)),
	}

	computed, err := rev.ComputeV1Alpha1Revision(revs)
	require.NoError(t, err)

	decoded, err := rev.DecodeV1Alpha1Revision(computed, decoder)
	require.NoError(t, err)

	require.Equal(t, revs, decoded)

	different := map[string]datastore.Revision{
		"foo": revisions.NewFromDecimal(decimal.NewFromInt(2)),
		"bar": revisions.NewFromDecimal(decimal.NewFromInt(7)),
	}

	computedDifferent, err := rev.ComputeV1Alpha1Revision(different)
	require.NoError(t, err)

	decodedDifferent, err := rev.DecodeV1Alpha1Revision(computedDifferent, decoder)
	require.NoError(t, err)

	require.Equal(t, different, decodedDifferent)
	require.NotEqual(t, revs, decodedDifferent)
}

func TestInvalidDecode(t *testing.T) {
	_, err := rev.DecodeV1Alpha1Revision("blahblah", decoder)
	require.Error(t, err)
}

func TestEncodeInObject(t *testing.T) {
	revisions := map[string]datastore.Revision{
		"foo":  revisions.NewFromDecimal(decimal.NewFromInt(1)),
		"bar":  revisions.NewFromDecimal(decimal.NewFromInt(2)),
		"baz":  revisions.NewFromDecimal(decimal.NewFromInt(3)),
		"meh":  revisions.NewFromDecimal(decimal.NewFromInt(4)),
		"yarg": revisions.NewFromDecimal(decimal.NewFromInt(5)),
	}

	// Encode the revision.
	computed, err := rev.ComputeV1Alpha1Revision(revisions)
	require.NoError(t, err)

	// Ensure it can be used as an object ID in the API.
	objRef := &v1.ObjectReference{
		ObjectType: "someobjecttype",
		ObjectId:   computed,
	}
	require.NoError(t, objRef.Validate())
}
