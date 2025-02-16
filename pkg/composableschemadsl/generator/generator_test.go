package generator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/composableschemadsl/compiler"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/namespace"
)

func TestGenerateCaveat(t *testing.T) {
	type generatorTest struct {
		name     string
		input    *core.CaveatDefinition
		expected string
		okay     bool
	}

	tests := []generatorTest{
		{
			"basic",
			namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
				map[string]caveattypes.VariableType{
					"someParam": caveattypes.IntType,
				},
			), "somecaveat", "someParam == 42"),
			`
caveat somecaveat(someParam int) {
	someParam == 42
}`,
			true,
		},
		{
			"multiparameter",
			namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
				map[string]caveattypes.VariableType{
					"someParam":    caveattypes.IntType,
					"anotherParam": caveattypes.MustMapType(caveattypes.UIntType),
				},
			), "somecaveat", "someParam == 42"),
			`
caveat somecaveat(anotherParam map<uint>, someParam int) {
	someParam == 42
}`,
			true,
		},
		{
			"long",
			namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
				map[string]caveattypes.VariableType{
					"someParam": caveattypes.IntType,
				},
			), "somecaveat", "someParam == 42 && someParam == 43 && someParam == 44 && someParam == 45"),
			`
caveat somecaveat(someParam int) {
	someParam == 42 && someParam == 43 && someParam == 44 && someParam == 45
}`,
			true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			source, ok, err := GenerateCaveatSource(test.input)
			require.NoError(err)
			require.Equal(strings.TrimSpace(test.expected), source)
			require.Equal(test.okay, ok)
		})
	}
}

func TestGenerateNamespace(t *testing.T) {
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
				namespace.MustRelation("somerel", nil, namespace.AllowedRelation("foos/bars", "hiya")),
			),
			`definition foos/test {
	relation somerel: foos/bars#hiya
}`,
			true,
		},
		{
			"simple permission",
			namespace.Namespace("foos/test",
				namespace.MustRelation("someperm", namespace.Union(
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
				namespace.MustRelation("someperm", namespace.Union(
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
				namespace.MustRelation("someperm", namespace.Union(
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
				namespace.MustRelation("somerel", namespace.Union(
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
				namespace.MustRelation("somerel", nil),
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
				namespace.MustRelation("owner", nil,
					namespace.AllowedRelation("foos/user", "..."),
				),
				namespace.MustRelationWithComment("reader", "//foobar", nil,
					namespace.AllowedRelation("foos/user", "..."),
					namespace.AllowedPublicNamespace("foos/user"),
					namespace.AllowedRelation("foos/group", "member"),
					namespace.AllowedRelationWithCaveat("foos/user", "...", namespace.AllowedCaveat("somecaveat")),
					namespace.AllowedRelationWithCaveat("foos/group", "member", namespace.AllowedCaveat("somecaveat")),
					namespace.AllowedPublicNamespaceWithCaveat("foos/user", namespace.AllowedCaveat("somecaveat")),
				),
				namespace.MustRelation("read", namespace.Union(
					namespace.ComputedUserset("reader"),
					namespace.ComputedUserset("owner"),
				)),
			),
			`/** Some comment goes here */
definition foos/document {
	relation owner: foos/user

	// foobar
	relation reader: foos/user | foos/user:* | foos/group#member | foos/user with somecaveat | foos/group#member with somecaveat | foos/user:* with somecaveat
	permission read = reader + owner
}`,
			true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			source, ok, err := GenerateSource(test.input)
			require.NoError(err)
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
/** some cool caveat */
caveat foos/somecaveat(someParam int, anotherParam bool) {
						someParam == 42 &&
				anotherParam
}

/** the document */
definition foos/document {
	/** some super long comment goes here and therefore should be made into a multiline comment */
	relation reader: foos/user | foos/user:* | foos/user with foos/somecaveat

	/** multiline
comment */
	relation  writer: foos/user

	// writers are also readers
	permission read = reader + writer + another
	permission write = writer
	permission minus = rela - relb - relc
}
`,
			`/** some cool caveat */
caveat foos/somecaveat(anotherParam bool, someParam int) {
	someParam == 42 && anotherParam
}

/** the document */
definition foos/document {
	/**
	 * some super long comment goes here and therefore should be made into a multiline comment
	 */
	relation reader: foos/user | foos/user:* | foos/user with foos/somecaveat

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
		{
			"different kinds of arrows",
			`definition document{
	permission first = rela->relb + relc.any(reld) + rele.all(relf)
}`,
			`definition document {
	permission first = rela->relb + relc.any(reld) + rele.all(relf)
}`,
		},
		{
			"expiration trait",
			`use expiration
			
			definition document{
				relation viewer: user with expiration
				relation editor: user with somecaveat and expiration
		}`,
			`use expiration

definition document {
	relation viewer: user with expiration
	relation editor: user with somecaveat and expiration
}`,
		},
		{
			"unused expiration flag",
			`use expiration
			
			definition document{
				relation viewer: user
		}`,
			`definition document {
	relation viewer: user
}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source(test.name),
				SchemaString: test.input,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			source, _, err := GenerateSchema(compiled.OrderedDefinitions)
			require.NoError(err)
			require.Equal(test.expected, source)
		})
	}
}
