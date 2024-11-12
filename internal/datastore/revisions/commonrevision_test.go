package revisions

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestRevisionByteSortable(t *testing.T) {
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

					if !leftRev.ByteSortable() || !rightRev.ByteSortable() {
						t.Skip("does not support byt sorting")
					}

					toSort := []string{leftRev.String(), rightRev.String()}
					sort.Strings(toSort)
					if tc.leftFirst {
						require.Equal(t, leftRev.String(), toSort[0])
						require.Equal(t, 0, bytes.Compare([]byte(leftRev.String()), []byte(toSort[0])))
					} else {
						require.Equal(t, rightRev.String(), toSort[0])
						require.Equal(t, 0, bytes.Compare([]byte(rightRev.String()), []byte(toSort[0])))
					}
				})
			}
		})
	}
}
