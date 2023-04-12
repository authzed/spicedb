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
		serialized:   "tenant/testns:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#testrel",
		objectFormat: ObjectAndRelation("tenant/testns", "veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong", "testrel"),
	},
	{
		serialized:   "tenant/testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#testrel",
		objectFormat: ObjectAndRelation("tenant/testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "testrel"),
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
		tc := tc
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
		tc := tc
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
		serialized:   "tenant/testns:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong",
		objectFormat: ObjectAndRelation("tenant/testns", "veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong", "..."),
	},
	{
		serialized:   "tenant/testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		objectFormat: ObjectAndRelation("tenant/testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "..."),
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
		tc := tc
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			parsed := ParseSubjectONR(tc.serialized)
			require.Equal(tc.objectFormat, parsed)
		})
	}
}
