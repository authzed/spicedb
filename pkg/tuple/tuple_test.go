package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"

	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
)

func makeTuple(onr *v0.ObjectAndRelation, userset *v0.ObjectAndRelation) *v0.RelationTuple {
	return &v0.RelationTuple{
		ObjectAndRelation: onr,
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: userset,
			},
		},
	}
}

var testCases = []struct {
	serialized   string
	objectFormat *v0.RelationTuple
}{
	{
		serialized: "tenant/testns:testobj#testrel@tenant/user:testusr#...",
		objectFormat: makeTuple(
			ObjectAndRelation("tenant/testns", "testobj", "testrel"),
			ObjectAndRelation("tenant/user", "testusr", "..."),
		),
	},
	{
		serialized:   "",
		objectFormat: nil,
	},
}

func TestSerialize(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			serialized := String(tc.objectFormat)
			require.Equal(tc.serialized, serialized)
		})
	}
}

func TestScan(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.serialized, func(t *testing.T) {
			require := require.New(t)

			parsed := Scan(tc.serialized)
			require.Equal(tc.objectFormat, parsed)
		})
	}
}
