package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var onrTestCases = []struct {
	serialized   string
	objectFormat ObjectAndRelation
}{
	{
		serialized:   "tenant/testns:testobj#testrel",
		objectFormat: StringToONR("tenant/testns", "testobj", "testrel"),
	},
	{
		serialized:   "tenant/testns:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#testrel",
		objectFormat: StringToONR("tenant/testns", "veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong", "testrel"),
	},
	{
		serialized:   "tenant/testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#testrel",
		objectFormat: StringToONR("tenant/testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "testrel"),
	},
	{
		serialized: "tenant/testns:*#testrel",
	},
	{
		serialized: "tenant/testns:testobj#...",
	},
	{
		serialized: "tenant/testns:testobj",
	},
	{
		serialized: "",
	},
}

func TestSerializeONR(t *testing.T) {
	for _, tc := range onrTestCases {
		tc := tc
		if tc.objectFormat.ObjectType == "" && tc.objectFormat.ObjectID == "" && tc.objectFormat.Relation == "" {
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

			parsed, err := ParseONR(tc.serialized)
			if tc.objectFormat.ObjectType == "" && tc.objectFormat.ObjectID == "" && tc.objectFormat.Relation == "" {
				require.Error(err)
				return
			}

			require.NoError(err)
			require.Equal(tc.objectFormat, parsed)
		})
	}
}

var subjectOnrTestCases = []struct {
	serialized   string
	objectFormat ObjectAndRelation
}{
	{
		serialized:   "tenant/testns:testobj#testrel",
		objectFormat: StringToONR("tenant/testns", "testobj", "testrel"),
	},
	{
		serialized:   "tenant/testns:testobj#...",
		objectFormat: StringToONR("tenant/testns", "testobj", "..."),
	},
	{
		serialized:   "tenant/testns:*#...",
		objectFormat: StringToONR("tenant/testns", "*", "..."),
	},
	{
		serialized:   "tenant/testns:testobj",
		objectFormat: StringToONR("tenant/testns", "testobj", "..."),
	},
	{
		serialized:   "tenant/testns:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong",
		objectFormat: StringToONR("tenant/testns", "veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong", "..."),
	},
	{
		serialized:   "tenant/testns:-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==",
		objectFormat: StringToONR("tenant/testns", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==", "..."),
	},
	{
		serialized: "tenant/testns:testobj#",
	},
	{
		serialized: "tenant/testns:testobj:",
	},
	{
		serialized: "",
	},
}

func TestParseSubjectONR(t *testing.T) {
	for _, tc := range subjectOnrTestCases {
		tc := tc
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			parsed, err := ParseSubjectONR(tc.serialized)
			if tc.objectFormat.ObjectType == "" && tc.objectFormat.ObjectID == "" && tc.objectFormat.Relation == "" {
				require.Error(err)
				return
			}

			require.Equal(tc.objectFormat, parsed)
		})
	}
}
