package revisions

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

var kinds = map[RevisionKind]bool{Timestamp: false, TransactionID: false, HybridLogicalClock: true}

func TestRevisionEqual(t *testing.T) {
	tcs := []struct {
		left    string
		right   string
		isEqual bool
	}{
		{
			"1",
			"2",
			false,
		},
		{
			"2",
			"1",
			false,
		},
		{
			"1",
			"1",
			true,
		},
		{
			"1.0000000004",
			"1",
			false,
		},
		{
			"1",
			"1.0000000004",
			false,
		},
		{
			"1.0000000004",
			"1.0000000004",
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.left+"-"+tc.right, func(t *testing.T) {
			for kind, supportsDecimals := range kinds {
				t.Run(string(kind), func(t *testing.T) {
					if !supportsDecimals && strings.Contains(tc.left, ".") {
						return
					}

					if !supportsDecimals && strings.Contains(tc.right, ".") {
						return
					}

					parser := RevisionParser(kind)

					leftRev, err := parser(tc.left)
					require.NoError(t, err)

					rightRev, err := parser(tc.right)
					require.NoError(t, err)

					require.Equal(t, tc.isEqual, leftRev.Equal(rightRev))
					require.Equal(t, tc.isEqual, rightRev.Equal(leftRev))
				})
			}
		})
	}
}

func TestRevisionComparison(t *testing.T) {
	tcs := []struct {
		left              string
		right             string
		isLeftGreaterThan bool
	}{
		{
			"1",
			"2",
			false,
		},
		{
			"2",
			"1",
			true,
		},
		{
			"1",
			"1",
			false,
		},
		{
			"1.0000000004",
			"1",
			true,
		},
		{
			"1",
			"1.0000000004",
			false,
		},
		{
			"1.0000000004",
			"1.0000000004",
			false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.left+"-"+tc.right, func(t *testing.T) {
			for kind, supportsDecimals := range kinds {
				t.Run(string(kind), func(t *testing.T) {
					if !supportsDecimals && strings.Contains(tc.left, ".") {
						return
					}

					if !supportsDecimals && strings.Contains(tc.right, ".") {
						return
					}

					parser := RevisionParser(kind)

					leftRev, err := parser(tc.left)
					require.NoError(t, err)

					rightRev, err := parser(tc.right)
					require.NoError(t, err)

					if leftRev.Equal(rightRev) {
						require.False(t, tc.isLeftGreaterThan)
						return
					}

					require.Equal(t, tc.isLeftGreaterThan, leftRev.GreaterThan(rightRev))
					require.Equal(t, !tc.isLeftGreaterThan, !leftRev.GreaterThan(rightRev))

					require.Equal(t, !tc.isLeftGreaterThan, leftRev.LessThan(rightRev))
					require.Equal(t, tc.isLeftGreaterThan, !leftRev.LessThan(rightRev))
				})
			}
		})
	}
}

func TestRevisionBidirectionalParsing(t *testing.T) {
	tcs := []string{
		"1.0000000000", "2.0000000000", "42.0000000000", "192747564535.0000000000", "1.0000000004", "1.0000000002", "1.0000000042", "-1235.0000000000",
	}

	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			for kind := range kinds {
				t.Run(string(kind), func(t *testing.T) {
					parser := RevisionParser(kind)
					parsed, err := parser(tc)
					if err != nil {
						return
					}

					require.Equal(t, tc, parsed.String())
				})
			}
		})
	}
}

func TestTimestampRevisionParsing(t *testing.T) {
	tcs := map[string]bool{
		"1":                   false,
		"2":                   false,
		"42":                  false,
		"1257894000000000000": false,
		"-1":                  false,
		"1.0000000004":        true,
	}

	for tc, expectError := range tcs {
		t.Run(tc, func(t *testing.T) {
			parser := RevisionParser(Timestamp)
			parsed, err := parser(tc)
			if expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc, parsed.String())
		})
	}
}

func TestTransactionIDRevisionParsing(t *testing.T) {
	tcs := map[string]bool{
		"1":                   false,
		"2":                   false,
		"42":                  false,
		"1257894000000000000": false,
		"-1":                  true,
		"1.0000000004":        true,
	}

	for tc, expectError := range tcs {
		t.Run(tc, func(t *testing.T) {
			parser := RevisionParser(TransactionID)
			parsed, err := parser(tc)
			if expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc, parsed.String())
		})
	}
}

func TestHLCRevisionParsing(t *testing.T) {
	tcs := map[string]bool{
		"1.0000000000":                   false,
		"2.0000000000":                   false,
		"42.0000000000":                  false,
		"1257894000000000000.0000000000": false,
		"-1.0000000000":                  false,
		"1.0000000004":                   false,
		"9223372036854775807.0000000004": false,
	}

	for tc, expectError := range tcs {
		t.Run(tc, func(t *testing.T) {
			parser := RevisionParser(HybridLogicalClock)
			parsed, err := parser(tc)
			if expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc, parsed.String())
		})
	}
}

func TestRevisionSortKeyOrdering(t *testing.T) {
	tcs := []struct {
		left      string
		right     string
		leftFirst bool
	}{
		{
			"1",
			"2",
			true,
		},
		{
			"2",
			"1",
			false,
		},
		{
			"1",
			"1",
			true,
		},
		{
			"1.0000000004",
			"1",
			false,
		},
		{
			"1",
			"1.0000000004",
			true,
		},
		{
			"1.0000000004",
			"1.0000000004",
			true,
		},
		{
			"1.1000000000",
			"1.0000000001",
			false,
		},
		{
			"9",
			"10",
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.left+"_"+tc.right, func(t *testing.T) {
			for kind, supportsDecimals := range kinds {
				t.Run(string(kind), func(t *testing.T) {
					if !supportsDecimals && strings.Contains(tc.left, ".") {
						t.Skip("does not support decimals")
					}

					if !supportsDecimals && strings.Contains(tc.right, ".") {
						t.Skip("does not support decimals")
					}
					parser := RevisionParser(kind)

					leftRev, err := parser(tc.left)
					require.NoError(t, err)

					rightRev, err := parser(tc.right)
					require.NoError(t, err)

					leftSK, ok := leftRev.(datastore.SortKeyRevision)
					if !ok {
						t.Skip("does not produce sort keys")
					}
					rightSK, ok := rightRev.(datastore.SortKeyRevision)
					if !ok {
						t.Skip("does not produce sort keys")
					}
					leftKey, rightKey := leftSK.AppendSortKey(nil), rightSK.AppendSortKey(nil)

					toSort := [][]byte{leftKey, rightKey}
					slices.SortFunc(toSort, bytes.Compare)
					if tc.leftFirst {
						require.Equal(t, 0, bytes.Compare(leftKey, toSort[0]))
					} else {
						require.Equal(t, 0, bytes.Compare(rightKey, toSort[0]))
					}
				})
			}
		})
	}
}

// sortKeyRandomIterations is how many random pairs each type's randomized ordering test draws.
const sortKeyRandomIterations = 2000

// newTestRNG returns a generator with a fixed seed, so a failure reproduces.
func newTestRNG() *rand.Rand {
	return rand.New(rand.NewPCG(0x5CE7, 0xDA7A)) //nolint:gosec // G404: a deterministically-seeded PRNG is exactly what is wanted here.
}

// requireSortKeyOrder takes revisions in ascending order and checks every pair: the keys sort the
// same way, they are all the expected width, and adding a suffix to each does not reorder them.
func requireSortKeyOrder(t *testing.T, ordered []datastore.SortKeyRevision, expectedLength int) {
	t.Helper()

	keys := make([][]byte, 0, len(ordered))
	for _, rev := range ordered {
		key := rev.AppendSortKey(nil)
		require.Len(t, key, expectedLength, "sort key for %s is not the fixed width", rev)
		keys = append(keys, key)
	}

	for i, left := range ordered {
		for j, right := range ordered {
			name := left.String() + "_vs_" + right.String()
			t.Run(name, func(t *testing.T) {
				cmp := bytes.Compare(keys[i], keys[j])
				switch {
				case i < j:
					require.True(t, left.LessThan(right))
					require.Negative(t, cmp, "sort key order disagrees with LessThan")
				case i > j:
					require.True(t, left.GreaterThan(right))
					require.Positive(t, cmp, "sort key order disagrees with GreaterThan")
				default:
					require.True(t, left.Equal(right))
					require.Zero(t, cmp, "sort keys of equal revisions differ")
				}

				if i == j {
					return
				}

				// No key is a prefix of another ...
				require.False(t, bytes.HasPrefix(keys[i], keys[j]))
				require.False(t, bytes.HasPrefix(keys[j], keys[i]))

				// ... so the worst case - highest possible suffix on the smaller revision,
				// lowest possible on the larger - still sorts correctly.
				leftComposite := append(append([]byte(nil), keys[i]...), 0xff)
				rightComposite := append(append([]byte(nil), keys[j]...), 0x00)
				require.Equal(t, cmp, bytes.Compare(leftComposite, rightComposite),
					"appending a suffix changed the relative order of the sort keys")
			})
		}
	}
}

// requireSortKeyAgreesWithComparators checks one pair of revisions, in either order.
func requireSortKeyAgreesWithComparators(t *testing.T, left, right datastore.SortKeyRevision) {
	t.Helper()

	cmp := bytes.Compare(left.AppendSortKey(nil), right.AppendSortKey(nil))
	switch {
	case left.LessThan(right):
		require.Negative(t, cmp, "%s < %s but sort keys say otherwise", left, right)
	case left.GreaterThan(right):
		require.Positive(t, cmp, "%s > %s but sort keys say otherwise", left, right)
	default:
		require.True(t, left.Equal(right))
		require.Zero(t, cmp, "%s == %s but sort keys differ", left, right)
	}
}

// TestAppendSortKeyAppends checks that dst is extended rather than overwritten, and that a nil dst
// yields a key on its own.
func TestAppendSortKeyAppends(t *testing.T) {
	rev := hlcFromString(t, 1739232000000000000, 7)

	standalone := rev.AppendSortKey(nil)
	require.Len(t, standalone, hlcSortKeyLength)

	prefix := []byte("relationship/")
	composite := rev.AppendSortKey(prefix)
	require.Equal(t, append(append([]byte(nil), prefix...), standalone...), composite)

	// Repeated appends to the same buffer are independent.
	twice := rev.AppendSortKey(composite)
	require.Equal(t, append(append([]byte(nil), composite...), standalone...), twice)
}

// TestSortKeyCapabilityDiscovery checks the type assertion a consumer makes where revisions enter
// it: a revision that can produce sort keys passes, one that cannot fails rather than quietly
// handing back something that does not sort.
func TestSortKeyCapabilityDiscovery(t *testing.T) {
	t.Run("supported", func(t *testing.T) {
		var rev datastore.Revision = TransactionIDRevision(1000)

		skr, ok := rev.(datastore.SortKeyRevision)
		require.True(t, ok)
		require.Equal(t, append([]byte("prefix/"), 0, 0, 0, 0, 0, 0, 0x03, 0xe8),
			skr.AppendSortKey([]byte("prefix/")))
	})

	t.Run("unsupported", func(t *testing.T) {
		_, ok := datastore.NoRevision.(datastore.SortKeyRevision)
		require.False(t, ok)
	})
}
