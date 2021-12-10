package tuple

import (
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"
)

var onrTestCases = []struct {
	serialized   string
	objectFormat *v0.ObjectAndRelation
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
	objectFormat *v0.ObjectAndRelation
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
