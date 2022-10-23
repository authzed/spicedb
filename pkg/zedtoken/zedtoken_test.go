package zedtoken

import (
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

var encodeRevisionTests = []datastore.Revision{
	revisions.NewFromDecimal(decimal.Zero),
	revisions.NewFromDecimal(decimal.NewFromInt(1)),
	revisions.NewFromDecimal(decimal.NewFromInt(2)),
	revisions.NewFromDecimal(decimal.NewFromInt(4)),
	revisions.NewFromDecimal(decimal.NewFromInt(8)),
	revisions.NewFromDecimal(decimal.NewFromInt(16)),
	revisions.NewFromDecimal(decimal.NewFromInt(128)),
	revisions.NewFromDecimal(decimal.NewFromInt(256)),
	revisions.NewFromDecimal(decimal.NewFromInt(1621538189028928000)),
	revisions.NewFromDecimal(decimal.New(12345, -2)),
	revisions.NewFromDecimal(decimal.New(0, -10)),
}

func TestZedTokenEncode(t *testing.T) {
	for _, rev := range encodeRevisionTests {
		t.Run(rev.String(), func(t *testing.T) {
			require := require.New(t)
			encoded := NewFromRevision(rev)
			decoded, err := DecodeRevision(encoded, revisions.DecimalDecoder{})
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
			}, revisions.DecimalDecoder{})
			if testCase.expectError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.True(
					revisions.NewFromDecimal(testCase.expectedRevision).Equal(decoded),
					"%s != %s",
					testCase.expectedRevision,
					decoded,
				)
			}
		})
	}
}
