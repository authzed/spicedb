package revisions

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestZeroTransactionIDRevision(t *testing.T) {
	require.False(t, TransactionIDRevision(0).LessThan(zeroTransactionIDRevision))
	require.True(t, TransactionIDRevision(0).Equal(zeroTransactionIDRevision))
	require.False(t, TransactionIDRevision(0).GreaterThan(zeroTransactionIDRevision))

	require.False(t, TransactionIDRevision(1).LessThan(zeroTransactionIDRevision))
	require.False(t, TransactionIDRevision(1).Equal(zeroTransactionIDRevision))
	require.True(t, TransactionIDRevision(1).GreaterThan(zeroTransactionIDRevision))
}

// TestTransactionIDStringOrderDisagreesWithRevisionOrder shows why sort keys are needed: String()
// is a decimal, so it does not sort in revision order. This is what a "sort key is just String()"
// implementation would get wrong.
func TestTransactionIDStringOrderDisagreesWithRevisionOrder(t *testing.T) {
	nine := TransactionIDRevision(9)
	ten := TransactionIDRevision(10)

	require.True(t, nine.LessThan(ten))
	require.Positive(t, bytes.Compare([]byte(nine.String()), []byte(ten.String())))
}

// TestTransactionIDStringIsNotPrefixFree shows the second, separate failure of String(), which
// every decimal revision form shares: "100" is a prefix of "1000", so once either becomes one field
// of a longer key, their order flips.
func TestTransactionIDStringIsNotPrefixFree(t *testing.T) {
	hundred := TransactionIDRevision(100)
	thousand := TransactionIDRevision(1000)

	require.True(t, hundred.LessThan(thousand))
	require.Equal(t, "100", hundred.String())
	require.Equal(t, "1000", thousand.String())
	require.True(t, bytes.HasPrefix([]byte(thousand.String()), []byte(hundred.String())),
		"expected %q to be a byte prefix of %q", hundred.String(), thousand.String())

	// "100z" sorts above "1000a", even though 100 < 1000.
	lower := append([]byte(hundred.String()), 'z')
	higher := append([]byte(thousand.String()), 'a')
	require.Positive(t, bytes.Compare(lower, higher),
		"expected the composite keys built from String() to be inverted")
}

// TestTransactionIDSortKeyOrderMatchesRevisionOrder is the positive counterpart: for the cases
// above and for the boundary values, sort keys sort the way the revisions compare.
func TestTransactionIDSortKeyOrderMatchesRevisionOrder(t *testing.T) {
	requireSortKeyOrder(t, []datastore.SortKeyRevision{
		TransactionIDRevision(0),
		TransactionIDRevision(9),
		TransactionIDRevision(10),
		TransactionIDRevision(100),
		TransactionIDRevision(1000),
		TransactionIDRevision(math.MaxInt64),
		TransactionIDRevision(math.MaxUint64),
	}, transactionIDSortKeyLength)
}

// TestTransactionIDSortKeyMatchesComparatorsRandomized repeats the ordering check on random values
// rather than hand-picked ones.
func TestTransactionIDSortKeyMatchesComparatorsRandomized(t *testing.T) {
	rng := newTestRNG()

	for range sortKeyRandomIterations {
		requireSortKeyAgreesWithComparators(t, TransactionIDRevision(rng.Uint64()), TransactionIDRevision(rng.Uint64()))
	}
}

// TestTransactionIDSortKeyEncodingIsStable pins the exact bytes. Sort keys may be stored durably,
// so changing any value here breaks existing data and must be deliberate.
func TestTransactionIDSortKeyEncodingIsStable(t *testing.T) {
	tcs := []struct {
		name     string
		rev      TransactionIDRevision
		expected []byte
	}{
		{
			"1000",
			TransactionIDRevision(1000),
			[]byte{0, 0, 0, 0, 0, 0, 0x03, 0xe8},
		},
		{
			"max",
			TransactionIDRevision(math.MaxUint64),
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.rev.AppendSortKey(nil))
		})
	}
}

// TestTransactionIDSortKeyWidthIsFixed checks that every key is the same width whatever the
// revision, which is what keeps one key from being a prefix of another.
func TestTransactionIDSortKeyWidthIsFixed(t *testing.T) {
	require.Equal(t, 8, transactionIDSortKeyLength)

	for _, rev := range []TransactionIDRevision{
		TransactionIDRevision(0),
		TransactionIDRevision(9),
		TransactionIDRevision(100),
		TransactionIDRevision(1000),
		TransactionIDRevision(math.MaxUint64),
	} {
		require.Len(t, rev.AppendSortKey(nil), transactionIDSortKeyLength, "revision %s", rev)
	}

	// String() has no such property: it runs from 1 byte to 20.
	require.Len(t, TransactionIDRevision(0).String(), 1)
	require.Len(t, TransactionIDRevision(math.MaxUint64).String(), 20)
}
