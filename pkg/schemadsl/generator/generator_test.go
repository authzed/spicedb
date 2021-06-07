package generator

import (
	"testing"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {

	type generatorTest struct {
		name     string
		input    *pb.NamespaceDefinition
		expected string
	}

	tests := []generatorTest{
		{
			"empty",
			namespace.Namespace("foo/test"),
			"definition test {}",
		},
		{
			"simple relation",
			namespace.Namespace("foo/test",
				namespace.Relation("somerel", nil, &pb.RelationReference{
					Namespace: "foo/bar",
					Relation:  "hiya",
				}),
			),
			`definition test {
	relation somerel: bar#hiya
}`,
		},
		{
			"simple permission",
			namespace.Namespace("foo/test",
				namespace.Relation("someperm", namespace.Union(
					namespace.ComputedUserset("anotherrel"),
				)),
			),
			`definition test {
	permission someperm = anotherrel
}`,
		},
		{
			"complex permission",
			namespace.Namespace("foo/test",
				namespace.Relation("someperm", namespace.Union(
					namespace.Rewrite(
						namespace.Exclusion(
							namespace.ComputedUserset("a"),
							namespace.ComputedUserset("b"),
							namespace.TupleToUserset("y", "z"),
						),
					),
					namespace.ComputedUserset("c"),
				)),
			),
			`definition test {
	permission someperm = (a - b - y->z) + c
}`,
		},
		{
			"legacy relation",
			namespace.Namespace("foo/test",
				namespace.Relation("somerel", namespace.Union(
					namespace.This(),
					namespace.ComputedUserset("anotherrel"),
				), &pb.RelationReference{
					Namespace: "foo/bar",
					Relation:  "hiya",
				}),
			),
			`definition test {
	relation somerel: bar#hiya = (/* _this unsupported here. Please rewrite into a relation and permission */) + anotherrel
}`,
		},
		{
			"missing type information",
			namespace.Namespace("foo/test",
				namespace.Relation("somerel", nil),
			),
			`definition test {
	relation somerel: /* missing allowed types */
}`,
		},

		{
			"full example",
			namespace.Namespace("foo/document",
				namespace.Relation("owner", nil,
					&pb.RelationReference{
						Namespace: "foo/user",
						Relation:  "...",
					},
				),
				namespace.Relation("reader", nil,
					&pb.RelationReference{
						Namespace: "foo/user",
						Relation:  "...",
					},
					&pb.RelationReference{
						Namespace: "foo/group",
						Relation:  "member",
					},
				),
				namespace.Relation("read", namespace.Union(
					namespace.ComputedUserset("reader"),
					namespace.ComputedUserset("owner"),
				)),
			),
			`definition document {
	relation owner: user
	relation reader: user | group#member
	permission read = reader + owner
}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			source := string(generateSource(test.input))
			require.Equal(test.expected, source)
		})
	}
}
