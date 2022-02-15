package tuple

import (
	"testing"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/stretchr/testify/require"
)

var onrTestCases = []struct {
	serialized   string
	objectFormat *core.ObjectAndRelation
}{
	{
		serialized:   "tenant/testns:testobj#testrel",
		objectFormat: ObjectAndRelation("tenant/testns", "testobj", "testrel"),
	},
	{
		serialized:   "tenant/testns:*#testrel",
		objectFormat: nil,
	},
	{
		serialized:   "tenant/testns:testobj#...",
		objectFormat: nil,
	},
	{
		serialized:   "tenant/testns:testobj",
		objectFormat: nil,
	},
	{
		serialized:   "",
		objectFormat: nil,
	},
}

func TestSerializeONR(t *testing.T) {
	for _, tc := range onrTestCases {
		if tc.objectFormat == nil {
			continue
		}

		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)
			serialized := StringONR(tc.objectFormat)
			require.Equal(tc.serialized, serialized)
		})
	}
}

func TestParseONR(t *testing.T) {
	for _, tc := range onrTestCases {
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			parsed := ParseONR(tc.serialized)
			require.Equal(tc.objectFormat, parsed)
		})
	}
}

var subjectOnrTestCases = []struct {
	serialized   string
	objectFormat *core.ObjectAndRelation
}{
	{
		serialized:   "tenant/testns:testobj#testrel",
		objectFormat: ObjectAndRelation("tenant/testns", "testobj", "testrel"),
	},
	{
		serialized:   "tenant/testns:testobj#...",
		objectFormat: ObjectAndRelation("tenant/testns", "testobj", "..."),
	},
	{
		serialized:   "tenant/testns:*#...",
		objectFormat: ObjectAndRelation("tenant/testns", "*", "..."),
	},
	{
		serialized:   "tenant/testns:testobj",
		objectFormat: ObjectAndRelation("tenant/testns", "testobj", "..."),
	},
	{
		serialized:   "tenant/testns:testobj#",
		objectFormat: nil,
	},
	{
		serialized:   "tenant/testns:testobj:",
		objectFormat: nil,
	},
	{
		serialized:   "",
		objectFormat: nil,
	},
}

func TestParseSubjectONR(t *testing.T) {
	for _, tc := range subjectOnrTestCases {
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			parsed := ParseSubjectONR(tc.serialized)
			require.Equal(tc.objectFormat, parsed)
		})
	}
}
