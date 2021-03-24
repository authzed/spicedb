package tuple

import (
	"testing"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/stretchr/testify/require"
)

var onrTestCases = []struct {
	serialized   string
	objectFormat *pb.ObjectAndRelation
}{
	{
		serialized:   "tenant/testns:testobj#testrel",
		objectFormat: ObjectAndRelation("tenant/testns", "testobj", "testrel"),
	},
	{
		serialized:   "",
		objectFormat: nil,
	},
}

func TestSerializeONR(t *testing.T) {
	for _, tc := range onrTestCases {
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			serialized := StringONR(tc.objectFormat)
			require.Equal(tc.serialized, serialized)
		})
	}
}

func TestScanONR(t *testing.T) {
	for _, tc := range onrTestCases {
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			parsed := ScanONR(tc.serialized)
			require.Equal(tc.objectFormat, parsed)
		})
	}
}
