package zookie

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

var encodeRevisionTests = []uint64{
	0, 1, 2, 4, 8, 16, 128, 256, ^uint64(0),
}

func TestZookieEncode(t *testing.T) {
	assert := assert.New(t)
	for _, rev := range encodeRevisionTests {
		encoded := NewFromRevision(rev)
		decoded, err := Decode(encoded)
		assert.Nil(err)
		assert.Equal(rev, decoded.GetV1().Revision)
	}
}

var decodeTests = []struct {
	token            string
	expectedRevision uint64
	expectError      bool
}{
	{
		token:            "abc",
		expectedRevision: 0,
		expectError:      true,
	},
	{
		token:            "CAESAA==",
		expectedRevision: 0,
		expectError:      false,
	},
	{
		token:            "CAESAggB",
		expectedRevision: 1,
		expectError:      false,
	},
	{
		token:            "CAESAggC",
		expectedRevision: 2,
		expectError:      false,
	},
	{
		token:            "CAESAwiAAg==",
		expectedRevision: 256,
		expectError:      false,
	},
}

func TestDecode(t *testing.T) {
	assert := assert.New(t)
	for _, testCase := range decodeTests {
		decoded, err := Decode(&pb.Zookie{
			Token: testCase.token,
		})
		if err == nil {
			assert.Equal(testCase.expectedRevision, decoded.GetV1().Revision)
		}

		assert.Equal(testCase.expectError, err != nil)
	}
}
