package generator

import (
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestGenerator(t *testing.T) {
	type generatorTest struct {
		name     string
		input    *v0.NamespaceDefinition
		expected string
		okay     bool
	}

	tests := []generatorTest{
		{
			"empty",
			namespace.Namespace("foo/test"),
			"definition foo/test {}",
			true,
		},
		{
			"simple relation",
			namespace.Namespace("foo/test",
				namespace.Relation("somerel", nil, &v0.RelationReference{
					Namespace: "foo/bar",
					Relation:  "hiya",
				}),
			),
			`definition foo/test {
	relation somerel: foo/bar#hiya
}`,
			true,
		},
		{
			"simple permission",
			namespace.Namespace("foo/test",
				namespace.Relation("someperm", namespace.Union(
					namespace.ComputedUserset("anotherrel"),
				)),
			),
			`definition foo/test {
	permission someperm = anotherrel
}`,
			true,
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
			`definition foo/test {
	permission someperm = (a - b - y->z) + c
}`,
			true,
		},
		{
			"legacy relation",
			namespace.Namespace("foo/test",
				namespace.Relation("somerel", namespace.Union(
					namespace.This(),
					namespace.ComputedUserset("anotherrel"),
				), &v0.RelationReference{
					Namespace: "foo/bar",
					Relation:  "hiya",
				}),
			),
			`definition foo/test {
	relation somerel: foo/bar#hiya = /* _this unsupported here. Please rewrite into a relation and permission */ + anotherrel
}`,
			false,
		},
		{
			"missing type information",
			namespace.Namespace("foo/test",
				namespace.Relation("somerel", nil),
			),
			`definition foo/test {
	relation somerel: /* missing allowed types */
}`,
			false,
		},

		{
			"full example",
			namespace.NamespaceWithComment("foo/document", `/**
* Some comment goes here
*/`,
				namespace.Relation("owner", nil,
					&v0.RelationReference{
						Namespace: "foo/user",
						Relation:  "...",
					},
				),
				namespace.RelationWithComment("reader", "//foobar", nil,
					&v0.RelationReference{
						Namespace: "foo/user",
						Relation:  "...",
					},
					&v0.RelationReference{
						Namespace: "foo/group",
						Relation:  "member",
					},
				),
				namespace.Relation("read", namespace.Union(
					namespace.ComputedUserset("reader"),
					namespace.ComputedUserset("owner"),
				)),
			),
			`/** Some comment goes here */
definition foo/document {
	relation owner: foo/user

	// foobar
	relation reader: foo/user | foo/group#member
	permission read = reader + owner
}`,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			source, ok := GenerateSource(test.input)
			require.Equal(test.expected, source)
			require.Equal(test.okay, ok)
		})
	}
}

func TestFormatting(t *testing.T) {
	type formattingTest struct {
		name     string
		input    string
		expected string
	}

	tests := []formattingTest{
		{
			"empty",
			"definition foo/test {}",
			"definition foo/test {}",
		},
		{
			"with comment",
			`/** some def */definition foo/test {}`,
			`/** some def */
definition foo/test {}`,
		},
		{
			"with rel comment",
			`/** some def */definition foo/test {

				// some rel
				relation somerel: foo/bar;
			}`,
			`/** some def */
definition foo/test {
	// some rel
	relation somerel: foo/bar
}`,
		},
		{
			"with multiple rel comment",
			`/** some def */definition foo/test {

				// some rel
				/* another comment */
				relation somerel: foo/bar;
			}`,
			`/** some def */
definition foo/test {
	// some rel
	/* another comment */
	relation somerel: foo/bar
}`,
		},
		{
			"with multiple rels with comment",
			`/** some def */definition foo/test {

				// some rel
				relation somerel: foo/bar;
				// another perm
				permission someperm = somerel
			}`,
			`/** some def */
definition foo/test {
	// some rel
	relation somerel: foo/bar

	// another perm
	permission someperm = somerel
}`,
		},

		{
			"becomes single line comment",
			`definition foo/test {
				/**
				 * hi there
				 */
				relation somerel: foo/bar;
			}`,
			`definition foo/test {
	/** hi there */
	relation somerel: foo/bar
}`,
		},

		{
			"full example",
			`
/** the document */
definition foo/document {
	/** some super long comment goes here and therefore should be made into a multiline comment */
	relation reader: foo/user

	/** multiline
comment */
	relation  writer: foo/user

	// writers are also readers
	permission read = reader + writer + another
	permission write = writer
	permission minus = a - b - c
}
`,
			`/** the document */
definition foo/document {
	/**
	 * some super long comment goes here and therefore should be made into a multiline comment
	 */
	relation reader: foo/user

	/**
	 * multiline
	 * comment
	 */
	relation writer: foo/user

	// writers are also readers
	permission read = reader + writer + another
	permission write = writer
	permission minus = (a - b) - c
}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			defs, err := compiler.Compile([]compiler.InputSchema{
				{input.InputSource(test.name), test.input},
			}, nil)
			require.NoError(err)

			source, _ := GenerateSource(defs[0])
			require.Equal(test.expected, source)
		})
	}
}
