package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	ONR = tuple.ObjectAndRelation
)

func TestSimplify(t *testing.T) {
	testCases := []struct {
		name     string
		tree     *pb.RelationTupleTreeNode
		expected []*pb.ObjectAndRelation
	}{
		{
			"simple leaf",
			Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
			[]*pb.ObjectAndRelation{ONR("user", "user1", "...")},
		},
		{
			"simple union",
			Union(nil,
				Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
				Leaf(nil, tuple.User(ONR("user", "user2", "..."))),
				Leaf(nil, tuple.User(ONR("user", "user3", "..."))),
			),
			[]*pb.ObjectAndRelation{
				ONR("user", "user1", "..."),
				ONR("user", "user2", "..."),
				ONR("user", "user3", "..."),
			},
		},
		{
			"simple intersection",
			Intersection(nil,
				Leaf(nil,
					tuple.User(ONR("user", "user1", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil,
					tuple.User(ONR("user", "user2", "...")),
					tuple.User(ONR("user", "user3", "...")),
				),
				Leaf(nil,
					tuple.User(ONR("user", "user2", "...")),
					tuple.User(ONR("user", "user4", "...")),
				),
			),
			[]*pb.ObjectAndRelation{ONR("user", "user2", "...")},
		},
		{
			"empty intersection",
			Intersection(nil,
				Leaf(nil,
					tuple.User(ONR("user", "user1", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil,
					tuple.User(ONR("user", "user3", "...")),
					tuple.User(ONR("user", "user4", "...")),
				),
			),
			[]*pb.ObjectAndRelation{},
		},
		{
			"simple exclusion",
			Exclusion(nil,
				Leaf(nil,
					tuple.User(ONR("user", "user1", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil, tuple.User(ONR("user", "user2", "..."))),
				Leaf(nil, tuple.User(ONR("user", "user3", "..."))),
			),
			[]*pb.ObjectAndRelation{ONR("user", "user1", "...")},
		},
		{
			"empty exclusion",
			Exclusion(nil,
				Leaf(nil,
					tuple.User(ONR("user", "user1", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
				Leaf(nil, tuple.User(ONR("user", "user2", "..."))),
			),
			[]*pb.ObjectAndRelation{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			var simplified UserSet = make(map[string]struct{})
			simplified.Add(Simplify(tc.tree)...)

			for _, onr := range tc.expected {
				usr := tuple.User(onr)
				require.True(simplified.Contains(usr))
				simplified.Remove(usr)
			}

			require.Len(simplified, 0)
		})
	}
}
