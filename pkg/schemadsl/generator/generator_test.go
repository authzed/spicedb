package generator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/decorators"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestNewSourceGenerator(t *testing.T) {
	mm := NewSourceGenerator(caveattypes.Default.TypeSet)
	require.NotNil(t, mm)
	require.True(t, mm.flags.IsEmpty())
}

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
			namespace.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"someParam": caveattypes.Default.IntType,
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
			namespace.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"someParam":    caveattypes.Default.IntType,
					"anotherParam": caveattypes.Default.MustMapType(caveattypes.Default.UIntType),
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
			namespace.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"someParam": caveattypes.Default.IntType,
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
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			source, _, ok, err := GenerateCaveatSource(test.input, caveattypes.Default.TypeSet)
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
			"complex permission with self",
			namespace.Namespace("foos/test",
				namespace.MustRelation("someperm", namespace.Union(
					namespace.Rewrite(
						namespace.Exclusion(
							namespace.ComputedUserset("rela"),
							namespace.ComputedUserset("relb"),
							namespace.TupleToUserset("rely", "relz"),
							namespace.Self(),
						),
					),
					namespace.ComputedUserset("relc"),
				)),
			),
			`definition foos/test {
	permission someperm = (rela - relb - rely->relz - self) + relc
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
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			source, ok, err := GenerateSource(test.input, caveattypes.Default.TypeSet)
			require.NoError(err)
			require.Equal(test.expected, source)
			require.Equal(test.okay, ok)
		})
	}
}

// TestFormatting asserts that the input schema gets turned into the output schema
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
			"expiration caveat",
			`definition document{
				relation viewer: user with expiration
		}`,
			`definition document {
	relation viewer: user with expiration
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
		{
			"use self happy path",
			`use self

			definition user {
				relation viewer: user
				permission view = viewer + self
			}`,
			`use self

definition user {
	relation viewer: user
	permission view = viewer + self
}`,
		},
		{
			"use self unused",
			`use self

			definition user {
				relation viewer: user
				permission view = viewer
			}`,
			`definition user {
	relation viewer: user
	permission view = viewer
}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source(test.name),
				SchemaString: test.input,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			source, _, err := GenerateSchema(t.Context(), compiled.OrderedDefinitions)
			require.NoError(err)
			require.Equal(test.expected, source)
		})
	}
}

func TestGenerateDecoratorsRoundTrip(t *testing.T) {
	t.Parallel()

	schema := `use testdecorators

@testall(needed: 7, label: "hi", on: true, mode: hash)
definition document {
	@testrel
	relation viewer: @testsub user

	@testrel
	permission view = viewer
}

definition user {}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.WithDecoratorRegistry(decorators.TestRegistry))
	require.NoError(t, err)

	generated, ok, err := GenerateSchema(t.Context(), compiled.OrderedDefinitions)
	require.NoError(t, err)
	require.True(t, ok)

	// The generated source must carry the decorators and the `use` line they require.
	require.Contains(t, generated, "use testdecorators")
	require.Contains(t, generated, "@testall(needed: 7, label: \"hi\", on: true, mode: hash)")
	require.Contains(t, generated, "@testrel")
	require.Contains(t, generated, "@testsub user")

	// And it must recompile to an identical schema.
	recompiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: generated,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.WithDecoratorRegistry(decorators.TestRegistry))
	require.NoError(t, err)

	regenerated, _, err := GenerateSchema(t.Context(), recompiled.OrderedDefinitions)
	require.NoError(t, err)
	require.Equal(t, generated, regenerated)
}

func TestGenerateDecoratorsOnCaveatRoundTrip(t *testing.T) {
	t.Parallel()

	schema := `use testdecorators

@testcaveat
caveat somecaveat(someparam int) {
	someparam == 42
}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.WithDecoratorRegistry(decorators.TestRegistry))
	require.NoError(t, err)

	generated, _, err := GenerateSchema(t.Context(), compiled.OrderedDefinitions)
	require.NoError(t, err)
	require.Contains(t, generated, "use testdecorators")
	require.Contains(t, generated, "@testcaveat")
}

// decoratorStringParam finds the named parameter on the given decorator and returns its
// StringValue, failing the test if the parameter is absent or not a string.
func decoratorStringParam(t *testing.T, d *core.Decorator, name string) string {
	t.Helper()
	for _, p := range d.GetParameters() {
		if p.GetName() == name {
			return p.GetStringValue()
		}
	}
	t.Fatalf("parameter %q not found on decorator %q", name, d.GetName())
	return ""
}

// TestGenerateDecoratorStringParameterQuoting exercises the DSL's lack of backslash-escape
// syntax: the generator must pick whichever of `"`/`'` the value does not contain, and must
// write the value out raw (no escaping) since the lexer never interprets a backslash. Each
// case asserts a full compile -> generate -> recompile -> regenerate round trip, checking
// both that the regenerated text is byte-identical to the first generation (a fixed point,
// which is what ComputeSchemaHash relies on for stability) and that the decoded StringValue
// survives unchanged.
func TestGenerateDecoratorStringParameterQuoting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		schema        string
		wantValue     string
		wantSubstring string
	}{
		{
			name: "double quote in value flips delimiter to single quote",
			schema: `use testdecorators

@testall(needed: 1, label: 'he said "hi"')
definition document {}`,
			wantValue:     `he said "hi"`,
			wantSubstring: `label: 'he said "hi"'`,
		},
		{
			name: "single quote in value keeps double quote delimiter",
			schema: `use testdecorators

@testall(needed: 1, label: "it's here")
definition document {}`,
			wantValue:     `it's here`,
			wantSubstring: `label: "it's here"`,
		},
		{
			name: "backslash requires no escaping",
			schema: `use testdecorators

@testall(needed: 1, label: "a\b")
definition document {}`,
			wantValue:     `a\b`,
			wantSubstring: `label: "a\b"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: test.schema,
			}, compiler.AllowUnprefixedObjectType(),
				compiler.WithDecoratorRegistry(decorators.TestRegistry))
			require.NoError(t, err)

			ns, ok := compiled.OrderedDefinitions[0].(*core.NamespaceDefinition)
			require.True(t, ok)
			require.Equal(t, test.wantValue, decoratorStringParam(t, ns.GetDecorators()[0], "label"))

			generated, ok, err := GenerateSchema(t.Context(), compiled.OrderedDefinitions)
			require.NoError(t, err)
			require.True(t, ok)
			require.Contains(t, generated, test.wantSubstring)

			recompiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: generated,
			}, compiler.AllowUnprefixedObjectType(),
				compiler.WithDecoratorRegistry(decorators.TestRegistry))
			require.NoError(t, err)

			recompiledNS, ok := recompiled.OrderedDefinitions[0].(*core.NamespaceDefinition)
			require.True(t, ok)
			require.Equal(t, test.wantValue, decoratorStringParam(t, recompiledNS.GetDecorators()[0], "label"),
				"recompiled StringValue must be byte-identical to the original")

			regenerated, _, err := GenerateSchema(t.Context(), recompiled.OrderedDefinitions)
			require.NoError(t, err)
			require.Equal(t, generated, regenerated)
		})
	}
}

// TestGenerateDecoratorParameterWithUnsetValueIsNotOK pins the defensive appendIssue guard
// in decoratorParameterValue's default case. decorators.Validate always sets one of the four
// oneof variants, so this path isn't reachable through the compiler, but a hand-built proto
// (or a future fifth oneof variant nobody wired up here yet) must not silently emit
// `@name(x: )`, which fails to recompile. It must flip `ok` to false instead.
func TestGenerateDecoratorParameterWithUnsetValueIsNotOK(t *testing.T) {
	t.Parallel()

	ns := namespace.Namespace("document")
	ns.Decorators = []*core.Decorator{
		{
			Name: "testdef",
			Parameters: []*core.DecoratorParameter{
				{Name: "x"}, // Value left unset.
			},
		},
	}

	_, ok, err := GenerateSource(ns, caveattypes.Default.TypeSet)
	require.NoError(t, err)
	require.False(t, ok)
}
