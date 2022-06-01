package generator

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestGenerator(t *testing.T) {
	type generatorTest struct {
		name     string
		input    *core.NamespaceDefinition
		expected string
		okay     bool
	}

	tests := []generatorTest{
		{
			"empty",
			namespace.Namespace("foos/test"),
			"definition foos/test {}",
			true,
		},
		{
			"simple relation",
			namespace.Namespace("foos/test",
				namespace.Relation("somerel", nil, namespace.AllowedRelation("foos/bars", "hiya")),
			),
			`definition foos/test {
	relation somerel: foos/bars#hiya
}`,
			true,
		},
		{
			"simple permission",
			namespace.Namespace("foos/test",
				namespace.Relation("someperm", namespace.Union(
					namespace.ComputedUserset("anotherrel"),
				)),
			),
			`definition foos/test {
	permission someperm = anotherrel
}`,
			true,
		},
		{
			"complex permission",
			namespace.Namespace("foos/test",
				namespace.Relation("someperm", namespace.Union(
					namespace.Rewrite(
						namespace.Exclusion(
							namespace.ComputedUserset("rela"),
							namespace.ComputedUserset("relb"),
							namespace.TupleToUserset("rely", "relz"),
						),
					),
					namespace.ComputedUserset("relc"),
				)),
			),
			`definition foos/test {
	permission someperm = (rela - relb - rely->relz) + relc
}`,
			true,
		},
		{
			"complex permission with nil",
			namespace.Namespace("foos/test",
				namespace.Relation("someperm", namespace.Union(
					namespace.Rewrite(
						namespace.Exclusion(
							namespace.ComputedUserset("rela"),
							namespace.ComputedUserset("relb"),
							namespace.TupleToUserset("rely", "relz"),
							namespace.Nil(),
						),
					),
					namespace.ComputedUserset("relc"),
				)),
			),
			`definition foos/test {
	permission someperm = (rela - relb - rely->relz - nil) + relc
}`,
			true,
		},
		{
			"legacy relation",
			namespace.Namespace("foos/test",
				namespace.Relation("somerel", namespace.Union(
					&core.SetOperation_Child{
						ChildType: &core.SetOperation_Child_XThis{},
					},
					namespace.ComputedUserset("anotherrel"),
				), namespace.AllowedRelation("foos/bars", "hiya")),
			),
			`definition foos/test {
	relation somerel: foos/bars#hiya = /* _this unsupported here. Please rewrite into a relation and permission */ + anotherrel
}`,
			false,
		},
		{
			"missing type information",
			namespace.Namespace("foos/test",
				namespace.Relation("somerel", nil),
			),
			`definition foos/test {
	relation somerel: /* missing allowed types */
}`,
			false,
		},

		{
			"full example",
			namespace.WithComment("foos/document", `/**
* Some comment goes here
*/`,
				namespace.Relation("owner", nil,
					namespace.AllowedRelation("foos/user", "..."),
				),
				namespace.RelationWithComment("reader", "//foobar", nil,
					namespace.AllowedRelation("foos/user", "..."),
					namespace.AllowedRelation("foos/group", "member"),
				),
				namespace.Relation("read", namespace.Union(
					namespace.ComputedUserset("reader"),
					namespace.ComputedUserset("owner"),
				)),
			),
			`/** Some comment goes here */
definition foos/document {
	relation owner: foos/user

	// foobar
	relation reader: foos/user | foos/group#member
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
			"definition foos/test {}",
			"definition foos/test {}",
		},
		{
			"with comment",
			`/** some def */definition foos/test {}`,
			`/** some def */
definition foos/test {}`,
		},
		{
			"with rel comment",
			`/** some def */definition foos/test {

				// some rel
				relation somerel: foos/bars;
			}`,
			`/** some def */
definition foos/test {
	// some rel
	relation somerel: foos/bars
}`,
		},
		{
			"with multiple rel comment",
			`/** some def */definition foos/test {

				// some rel
				/* another comment */
				relation somerel: foos/bars;
			}`,
			`/** some def */
definition foos/test {
	// some rel
	/* another comment */
	relation somerel: foos/bars
}`,
		},
		{
			"with multiple rels with comment",
			`/** some def */definition foos/test {

				// some rel
				relation somerel: foos/bars;
				// another perm
				permission someperm = somerel
			}`,
			`/** some def */
definition foos/test {
	// some rel
	relation somerel: foos/bars

	// another perm
	permission someperm = somerel
}`,
		},

		{
			"becomes single line comment",
			`definition foos/test {
				/**
				 * hi there
				 */
				relation somerel: foos/bars;
			}`,
			`definition foos/test {
	/** hi there */
	relation somerel: foos/bars
}`,
		},

		{
			"full example",
			`
/** the document */
definition foos/document {
	/** some super long comment goes here and therefore should be made into a multiline comment */
	relation reader: foos/user | foos/user:*

	/** multiline
comment */
	relation  writer: foos/user

	// writers are also readers
	permission read = reader + writer + another
	permission write = writer
	permission minus = rela - relb - relc
}
`,
			`/** the document */
definition foos/document {
	/**
	 * some super long comment goes here and therefore should be made into a multiline comment
	 */
	relation reader: foos/user | foos/user:*

	/**
	 * multiline
	 * comment
	 */
	relation writer: foos/user

	// writers are also readers
	permission read = reader + writer + another
	permission write = writer
	permission minus = (rela - relb) - relc
}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			defs, err := compiler.Compile([]compiler.InputSchema{{
				Source:       input.Source(test.name),
				SchemaString: test.input,
			}}, nil)
			require.NoError(err)

			source, _ := GenerateSource(defs[0])
			require.Equal(test.expected, source)
		})
	}
}
