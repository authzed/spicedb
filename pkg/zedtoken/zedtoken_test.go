package zedtoken

import (
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
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
	format           string
	token            string
	expectedRevision decimal.Decimal
	expectError      bool
}{
	{
		format:           "invalid",
		token:            "abc",
		expectedRevision: decimal.Zero,
		expectError:      true,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAA==",
		expectedRevision: decimal.Zero,
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAggB",
		expectedRevision: decimal.NewFromInt(1),
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAggC",
		expectedRevision: decimal.NewFromInt(2),
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAwiAAg==",
		expectedRevision: decimal.NewFromInt(256),
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAIaAwoBMA==",
		expectedRevision: decimal.Zero,
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaAwoBMQ==",
		expectedRevision: decimal.NewFromInt(1),
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaAwoBMg==",
		expectedRevision: decimal.NewFromInt(2),
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaAwoBNA==",
		expectedRevision: decimal.NewFromInt(4),
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaFQoTMTYyMTUzODE4OTAyODkyODAwMA==",
		expectedRevision: decimal.NewFromInt(1621538189028928000),
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaCAoGMTIzLjQ1",
		expectedRevision: decimal.New(12345, -2),
		expectError:      false,
	},
}

func TestDecode(t *testing.T) {
	for _, testCase := range decodeTests {
		testName := fmt.Sprintf("%s(%s)=>%s", testCase.format, testCase.token, testCase.expectedRevision)
		t.Run(testName, func(t *testing.T) {
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
