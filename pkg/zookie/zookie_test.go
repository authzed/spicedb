package zookie

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
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
	decimal.NewFromInt((time.Now().UnixNano())),
	decimal.New(12345, -2),
	decimal.New(0, -10),
}

func TestZookieEncode(t *testing.T) {
	assert := assert.New(t)
	for _, rev := range encodeRevisionTests {
		encoded := NewFromRevision(rev)
		decoded, err := DecodeRevision(encoded)
		assert.Nil(err)
		assert.True(rev.Equal(decoded))
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
		token:            "CAESAA==",
		expectedRevision: decimal.Zero,
		expectError:      false,
	},
	{
		token:            "CAESAggB",
		expectedRevision: decimal.NewFromInt(1),
		expectError:      false,
	},
	{
		token:            "CAESAggC",
		expectedRevision: decimal.NewFromInt(2),
		expectError:      false,
	},
	{
		token:            "CAESAwiAAg==",
		expectedRevision: decimal.NewFromInt(256),
		expectError:      false,
	},
	{
		token:            "CAIaAwoBMA==",
		expectedRevision: decimal.Zero,
		expectError:      false,
	},
	{
		token:            "CAIaAwoBMQ==",
		expectedRevision: decimal.NewFromInt(1),
		expectError:      false,
	},
	{
		token:            "CAIaAwoBMg==",
		expectedRevision: decimal.NewFromInt(2),
		expectError:      false,
	},
	{
		token:            "CAIaAwoBNA==",
		expectedRevision: decimal.NewFromInt(4),
		expectError:      false,
	},
	{
		token:            "CAIaFQoTMTYyMTUzODE4OTAyODkyODAwMA==",
		expectedRevision: decimal.NewFromInt(1621538189028928000),
		expectError:      false,
	},
	{
		token:            "CAIaCAoGMTIzLjQ1",
		expectedRevision: decimal.New(12345, -2),
		expectError:      false,
	},
	{
		token:            "CAIaAwoBMA==",
		expectedRevision: decimal.Zero,
		expectError:      false,
	},
}

func TestDecode(t *testing.T) {
	assert := assert.New(t)
	for _, testCase := range decodeTests {
		decoded, err := DecodeRevision(&pb.Zookie{
			Token: testCase.token,
		})
		if err == nil {
			assert.True(
				testCase.expectedRevision.Equal(decoded),
				"%s != %s",
				testCase.expectedRevision,
				decoded,
			)
		}

		assert.Equal(testCase.expectError, err != nil)
	}
}
