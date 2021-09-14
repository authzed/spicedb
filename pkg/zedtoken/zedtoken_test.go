package zedtoken

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/spicedb/internal/proto/authzed/api/v1"
)

var encodeRevisionTests = []decimal.Decimal{
	decimal.Zero,
	decimal.NewFromInt(1),
	decimal.NewFromInt(2),
	decimal.NewFromInt(4),
	decimal.NewFromInt(8),
	decimal.NewFromInt(16),
	decimal.NewFromInt(128),
	decimal.NewFromInt(256),
	decimal.NewFromInt(1621538189028928000),
	decimal.New(12345, -2),
	decimal.New(0, -10),
}

func TestZedTokenEncode(t *testing.T) {
	for _, rev := range encodeRevisionTests {
		t.Run(rev.String(), func(t *testing.T) {
			require := require.New(t)
			encoded := NewFromRevision(rev)
			decoded, err := DecodeRevision(encoded)
			require.NoError(err)
			require.True(rev.Equal(decoded))
		})
	}
}

var decodeTests = []struct {
	token            string
	expectedRevision decimal.Decimal
	expectError      bool
}{
	{
		token:            "abc",
		expectedRevision: decimal.Zero,
		expectError:      true,
	},
	{
		token:            "CAESAwoBMA==",
		expectedRevision: decimal.Zero,
		expectError:      false,
	},
	{
		token:            "CAESAwoBMQ==",
		expectedRevision: decimal.NewFromInt(1),
		expectError:      false,
	},
	{
		token:            "CAESAwoBMg==",
		expectedRevision: decimal.NewFromInt(2),
		expectError:      false,
	},
	{
		token:            "CAESBQoDMjU2",
		expectedRevision: decimal.NewFromInt(256),
		expectError:      false,
	},
	{
		token:            "CAESFQoTMTYyMTUzODE4OTAyODkyODAwMA==",
		expectedRevision: decimal.NewFromInt(1621538189028928000),
		expectError:      false,
	},
	{
		token:            "CAESCAoGMTIzLjQ1",
		expectedRevision: decimal.New(12345, -2),
		expectError:      false,
	},
}

func TestDecode(t *testing.T) {
	for _, testCase := range decodeTests {
		t.Run(testCase.token, func(t *testing.T) {
			require := require.New(t)

			decoded, err := DecodeRevision(&v1.ZedToken{
				Token: testCase.token,
			})
			if testCase.expectError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.True(
					testCase.expectedRevision.Equal(decoded),
					"%s != %s",
					testCase.expectedRevision,
					decoded,
				)
			}
		})
	}
}
