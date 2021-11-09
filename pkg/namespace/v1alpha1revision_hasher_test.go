package namespace

import (
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestHashingSame(t *testing.T) {
	encoded, err := ComputeV1Alpha1Revision(map[string]decimal.Decimal{
		"foo": decimal.NewFromInt(42),
		"bar": decimal.NewFromInt(16),
	})
	require.NoError(t, err)

	hash, err := ComputeHashForRevision(encoded)
	require.NoError(t, err)

	hash2, err := ComputeHashForRevision(encoded)
	require.NoError(t, err)

	require.Equal(t, hash, hash2)
}

func TestHashingDifferent(t *testing.T) {
	encoded, err := ComputeV1Alpha1Revision(map[string]decimal.Decimal{
		"foo": decimal.NewFromInt(42),
		"bar": decimal.NewFromInt(16),
	})
	require.NoError(t, err)

	encoded2, err := ComputeV1Alpha1Revision(map[string]decimal.Decimal{
		"foo": decimal.NewFromInt(43),
		"bar": decimal.NewFromInt(16),
	})
	require.NoError(t, err)

	hash, err := ComputeHashForRevision(encoded)
	require.NoError(t, err)

	hash2, err := ComputeHashForRevision(encoded2)
	require.NoError(t, err)

	require.NotEqual(t, hash, hash2)
}

func TestHashingStable(t *testing.T) {
	testCases := []struct {
		input    map[string]decimal.Decimal
		expected string
	}{
		{
			input: map[string]decimal.Decimal{
				"foo": decimal.NewFromInt(42),
				"bar": decimal.NewFromInt(16),
			},
			expected: "7718a08c8ab2dd38eb5f5b47932c8c05204cebfbc6a57a8b34fc614b3c17fcc2",
		},
		{
			input: map[string]decimal.Decimal{
				"faa": decimal.NewFromInt(42),
				"bar": decimal.NewFromInt(16),
			},
			expected: "cda4e43ebd43dbbaf39bfc598cc060ce6e0670e0db9221d1a0d75ea1f2703537",
		},
		{
			input: map[string]decimal.Decimal{
				"foo/bar": decimal.NewFromInt(42),
			},
			expected: "8b2a162d68780bac1fe325a7613c56c9dd8b0b7fa8cf3e945f132218dde6d7e4",
		},
	}

	for index, tc := range testCases {
		t.Run(fmt.Sprintf("%d", index), func(t *testing.T) {
			encoded, err := ComputeV1Alpha1Revision(tc.input)
			require.NoError(t, err)

			hash, err := ComputeHashForRevision(encoded)
			require.NoError(t, err)

			require.Equal(t, tc.expected, hash)
		})
	}
}
