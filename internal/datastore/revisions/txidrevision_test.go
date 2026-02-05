package revisions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZeroTransactionIDRevision(t *testing.T) {
	require.False(t, TransactionIDRevision(0).LessThan(zeroTransactionIDRevision))
	require.True(t, TransactionIDRevision(0).Equal(zeroTransactionIDRevision))
	require.False(t, TransactionIDRevision(0).GreaterThan(zeroTransactionIDRevision))

	require.False(t, TransactionIDRevision(1).LessThan(zeroTransactionIDRevision))
	require.False(t, TransactionIDRevision(1).Equal(zeroTransactionIDRevision))
	require.True(t, TransactionIDRevision(1).GreaterThan(zeroTransactionIDRevision))
}

func TestTransactionIDRevisionKey(t *testing.T) {
	testCases := []struct {
		name string
		txID uint64
	}{
		{
			name: "zero",
			txID: 0,
		},
		{
			name: "small number",
			txID: 42,
		},
		{
			name: "large number",
			txID: 1234567890123456789,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rev := NewForTransactionID(tc.txID)

			// Key should be deterministic
			key1 := rev.Key()
			key2 := rev.Key()
			require.Equal(t, key1, key2, "Key() should be deterministic")

			// Key should equal String() for transaction ID revisions
			require.Equal(t, rev.String(), key1, "Key() should match String() for transaction ID revisions")

			// Equal revisions should have equal keys
			rev2 := NewForTransactionID(tc.txID)
			require.Equal(t, rev.Key(), rev2.Key(), "Equal revisions should have equal keys")

			// Different revisions should have different keys
			rev3 := NewForTransactionID(tc.txID + 1)
			require.NotEqual(t, rev.Key(), rev3.Key(), "Different revisions should have different keys")
		})
	}
}
