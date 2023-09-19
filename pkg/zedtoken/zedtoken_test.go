package zedtoken

import (
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

var encodeRevisionTests = []datastore.Revision{
	revision.NewFromDecimal(decimal.Zero),
	revision.NewFromDecimal(decimal.NewFromInt(1)),
	revision.NewFromDecimal(decimal.NewFromInt(2)),
	revision.NewFromDecimal(decimal.NewFromInt(4)),
	revision.NewFromDecimal(decimal.NewFromInt(8)),
	revision.NewFromDecimal(decimal.NewFromInt(16)),
	revision.NewFromDecimal(decimal.NewFromInt(128)),
	revision.NewFromDecimal(decimal.NewFromInt(256)),
	revision.NewFromDecimal(decimal.NewFromInt(1621538189028928000)),
	revision.NewFromDecimal(decimal.New(12345, -2)),
	revision.NewFromDecimal(decimal.New(0, -10)),
}

func TestZedTokenEncode(t *testing.T) {
	for _, rev := range encodeRevisionTests {
		rev := rev
		t.Run(rev.String(), func(t *testing.T) {
			require := require.New(t)
			encoded, err := NewFromRevision(rev)
			require.NoError(err)

			decoded, err := DecodeRevision(encoded, revision.DecimalDecoder{})
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
	{
		format: "V1 ZedToken",
		token:  "GiAKHjE2OTM1NDA5NDAzNzMwNDU3MjcuMDAwMDAwMDAwMQ==",
		expectedRevision: (func() decimal.Decimal {
			v, err := decimal.NewFromString("1693540940373045727.0000000001")
			if err != nil {
				panic(err)
			}
			return v
		})(),
		expectError: false,
	},
}

func TestDecode(t *testing.T) {
	for _, testCase := range decodeTests {
		testCase := testCase
		testName := fmt.Sprintf("%s(%s)=>%s", testCase.format, testCase.token, testCase.expectedRevision)
		t.Run(testName, func(t *testing.T) {
			require := require.New(t)

			decoded, err := DecodeRevision(&v1.ZedToken{
				Token: testCase.token,
			}, revision.DecimalDecoder{})
			if testCase.expectError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.True(
					revision.NewFromDecimal(testCase.expectedRevision).Equal(decoded),
					"%s != %s",
					testCase.expectedRevision,
					decoded,
				)
			}
		})
	}
}
