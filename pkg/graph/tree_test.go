package graph

import (
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/tuple"
)

var ONR = tuple.ObjectAndRelation

func TestSimplify(t *testing.T) {
	testCases := []struct {
		name     string
		tree     *v0.RelationTupleTreeNode
		expected []*v0.ObjectAndRelation
	}{
		{
			"simple leaf",
			Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
			[]*v0.ObjectAndRelation{ONR("user", "user1", "...")},
		},
		{
			"simple union",
			Union(nil,
				Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
				Leaf(nil, tuple.User(ONR("user", "user2", "..."))),
				Leaf(nil, tuple.User(ONR("user", "user3", "..."))),
			),
			[]*v0.ObjectAndRelation{
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
			[]*v0.ObjectAndRelation{ONR("user", "user2", "...")},
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
			[]*v0.ObjectAndRelation{},
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
			[]*v0.ObjectAndRelation{ONR("user", "user1", "...")},
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
			[]*v0.ObjectAndRelation{},
		},
		{
			"wildcard left side exclusion",
			Exclusion(nil,
				Leaf(nil,
					tuple.User(ONR("user", "*", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
			),
			[]*v0.ObjectAndRelation{
				ONR("user", "user2", "..."),
				ONR("user", "*", "..."),
			},
		},
		{
			"wildcard right side exclusion",
			Exclusion(nil,
				Leaf(nil,
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil, tuple.User(ONR("user", "*", "..."))),
			),
			[]*v0.ObjectAndRelation{},
		},
		{
			"wildcard both sides exclusion",
			Exclusion(nil,
				Leaf(nil,
					tuple.User(ONR("user", "user2", "...")),
					tuple.User(ONR("user", "*", "...")),
				),
				Leaf(nil, tuple.User(ONR("user", "*", "..."))),
			),
			[]*v0.ObjectAndRelation{},
		},
		{
			"wildcard left side intersection",
			Intersection(nil,
				Leaf(nil,
					tuple.User(ONR("user", "*", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
				Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
			),
			[]*v0.ObjectAndRelation{
				ONR("user", "user1", "..."),
			},
		},
		{
			"wildcard right side intersection",
			Intersection(nil,
				Leaf(nil, tuple.User(ONR("user", "user1", "..."))),
				Leaf(nil,
					tuple.User(ONR("user", "*", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
			),
			[]*v0.ObjectAndRelation{
				ONR("user", "user1", "..."),
			},
		},
		{
			"wildcard both sides intersection",
			Intersection(nil,
				Leaf(nil,
					tuple.User(ONR("user", "*", "...")),
					tuple.User(ONR("user", "user1", "..."))),
				Leaf(nil,
					tuple.User(ONR("user", "*", "...")),
					tuple.User(ONR("user", "user2", "...")),
				),
			),
			[]*v0.ObjectAndRelation{
				ONR("user", "user1", "..."),
				ONR("user", "*", "..."),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			simplified := Simplify(tc.tree)
			for _, onr := range tc.expected {
				require.True(simplified.Contains(onr), "missing expected subject %s", onr)
				simplified.Remove(onr)
			}

			require.Len(simplified, 0)
		})
	}
}
