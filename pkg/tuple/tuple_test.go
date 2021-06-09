package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

func makeTuple(onr *pb.ObjectAndRelation, userset *pb.ObjectAndRelation) *pb.RelationTuple {
	return &pb.RelationTuple{
		ObjectAndRelation: onr,
		User: &pb.User{
			UserOneof: &pb.User_Userset{
				Userset: userset,
			},
		},
	}
}

var testCases = []struct {
	serialized   string
	objectFormat *pb.RelationTuple
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
