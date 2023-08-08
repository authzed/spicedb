package compiler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
)

var someTenant = "sometenant"

func TestCompile(t *testing.T) {
	type compileTest struct {
		name           string
		implicitTenant *string
		input          string
		expectedError  string
		expectedProto  []SchemaDefinition
	}

	tests := []compileTest{
		{
			"empty",
			&someTenant,
			"",
			"",
			[]SchemaDefinition{},
		},
		{
			"parse error",
			&someTenant,
			"foo",
			"parse error in `parse error`, line 1, column 1: Unexpected token at root level: TokenTypeIdentifier",
			[]SchemaDefinition{},
		},
		{
			"nested parse error",
			&someTenant,
			`definition foo {
				relation something: rela | relb + relc	
			}`,
			"parse error in `nested parse error`, line 2, column 37: Expected end of statement or definition, found: TokenTypePlus",
			[]SchemaDefinition{},
		},
		{
			"empty def",
			&someTenant,
			`definition def {}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/def"),
			},
		},
		{
			"simple def",
			&someTenant,
			`definition simple {
				relation foo: bar;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foo", nil,
						namespace.AllowedRelation("sometenant/bar", "..."),
					),
				),
			},
		},
		{
			"explicit relation",
			&someTenant,
			`definition simple {
				relation foos: bars#mehs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos", nil,
						namespace.AllowedRelation("sometenant/bars", "mehs"),
					),
				),
			},
		},
		{
			"wildcard relation",
			&someTenant,
			`definition simple {
				relation foos: bars:*
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos", nil,
						namespace.AllowedPublicNamespace("sometenant/bars"),
					),
				),
			},
		},
		{
			"cross tenant relation",
			&someTenant,
			`definition simple {
				relation foos: anothertenant/bars#mehs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos", nil,
						namespace.AllowedRelation("anothertenant/bars", "mehs"),
					),
				),
			},
		},
		{
			"multiple relations",
			&someTenant,
			`definition simple {
				relation foos: bars#mehs;
				relation hello: there | world;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos", nil,
						namespace.AllowedRelation("sometenant/bars", "mehs"),
					),
					namespace.MustRelation("hello", nil,
						namespace.AllowedRelation("sometenant/there", "..."),
						namespace.AllowedRelation("sometenant/world", "..."),
					),
				),
			},
		},
		{
			"relation with required caveat",
			&someTenant,
			`definition simple {
				relation viewer: user with somecaveat
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveat("sometenant/user", "...",
							namespace.AllowedCaveat("somecaveat")),
					),
				),
			},
		},
		{
			"relation with optional caveat",
			&someTenant,
			`definition simple {
				relation viewer: user with somecaveat | user
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveat("sometenant/user", "...",
							namespace.AllowedCaveat("somecaveat")),
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"relation with multiple caveats",
			&someTenant,
			`definition simple {
				relation viewer: user with somecaveat | user | team#member with anothercaveat
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveat("sometenant/user", "...",
							namespace.AllowedCaveat("somecaveat")),
						namespace.AllowedRelation("sometenant/user", "..."),
						namespace.AllowedRelationWithCaveat("sometenant/team", "member",
							namespace.AllowedCaveat("anothercaveat")),
					),
				),
			},
		},
		{
			"simple permission",
			&someTenant,
			`definition simple {
				permission foos = bars;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("bars"),
						),
					),
				),
			},
		},
		{
			"union permission",
			&someTenant,
			`definition simple {
				permission foos = bars + bazs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
				),
			},
		},
		{
			"intersection permission",
			&someTenant,
			`definition simple {
				permission foos = bars & bazs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Intersection(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
				),
			},
		},
		{
			"exclusion permission",
			&someTenant,
			`definition simple {
				permission foos = bars - bazs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Exclusion(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
				),
			},
		},
		{
			"multi-union permission",
			&someTenant,
			`definition simple {
				permission foos = bars + bazs + mehs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
							namespace.ComputedUserset("mehs"),
						),
					),
				),
			},
		},
		{
			"complex permission",
			&someTenant,
			`definition complex {
				permission foos = bars + bazs - mehs;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/complex",
					namespace.MustRelation("foos",
						namespace.Exclusion(
							namespace.Rewrite(
								namespace.Union(
									namespace.ComputedUserset("bars"),
									namespace.ComputedUserset("bazs"),
								),
							),
							namespace.ComputedUserset("mehs"),
						),
					),
				),
			},
		},
		{
			"complex parens permission",
			&someTenant,
			`definition complex {
				permission foos = bars + (bazs - mehs);
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/complex",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("bars"),
							namespace.Rewrite(
								namespace.Exclusion(
									namespace.ComputedUserset("bazs"),
									namespace.ComputedUserset("mehs"),
								),
							),
						),
					),
				),
			},
		},
		{
			"arrow permission",
			&someTenant,
			`definition arrowed {
				permission foos = bars->bazs
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/arrowed",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.TupleToUserset("bars", "bazs"),
						),
					),
				),
			},
		},

		{
			"multiarrow permission",
			&someTenant,
			`definition arrowed {
				relation somerel: something;
				permission foos = somerel->brel->crel
			}`,
			"parse error in `multiarrow permission`, line 3, column 23: Nested arrows not yet supported",
			[]SchemaDefinition{},
		},

		/*
			 * TODO: uncomment once supported and remove the test above
			{
				"multiarrow permission",
				`definition arrowed {
					relation somerel: something;
					permission foo = somerel->brel->crel
				}`,
				"",
				[]SchemaDefinition{
					namespace.Namespace("sometenant/arrowed",
						namespace.MustRelation("foo",
							namespace.Union(
								namespace.TupleToUserset("bar", "baz"),
							),
						),
					),
				},
			},
		*/

		{
			"expression permission",
			&someTenant,
			`definition expressioned {
				permission foos = ((arel->brel) + nil) - drel
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/expressioned",
					namespace.MustRelation("foos",
						namespace.Exclusion(
							namespace.Rewrite(
								namespace.Union(
									namespace.TupleToUserset("arel", "brel"),
									namespace.Nil(),
								),
							),
							namespace.ComputedUserset("drel"),
						),
					),
				),
			},
		},
		{
			"multiple permission",
			&someTenant,
			`definition multiple {
				permission first = bars + bazs
				permission second = bars - bazs
				permission third = bars & bazs
				permission fourth = bars->bazs
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/multiple",
					namespace.MustRelation("first",
						namespace.Union(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
					namespace.MustRelation("second",
						namespace.Exclusion(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
					namespace.MustRelation("third",
						namespace.Intersection(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
					namespace.MustRelation("fourth",
						namespace.Union(
							namespace.TupleToUserset("bars", "bazs"),
						),
					),
				),
			},
		},
		{
			"permission with nil",
			&someTenant,
			`definition simple {
				permission foos = aaaa + nil + bbbb;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("aaaa"),
							namespace.Nil(),
							namespace.ComputedUserset("bbbb"),
						),
					),
				),
			},
		},
		{
			"no implicit tenant with unspecified tenant",
			nil,
			`definition foos {}`,
			"parse error in `no implicit tenant with unspecified tenant`, line 1, column 1: found reference `foos` without prefix",
			[]SchemaDefinition{},
		},
		{
			"no implicit tenant with specified tenant",
			nil,
			`definition some_tenant/foos {}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("some_tenant/foos"),
			},
		},
		{
			"no implicit tenant with unspecified tenant on type ref",
			nil,
			`definition some_tenant/foo {
				relation somerel: bars
			}`,
			"parse error in `no implicit tenant with unspecified tenant on type ref`, line 2, column 23: found reference `bars` without prefix",
			[]SchemaDefinition{},
		},
		{
			"invalid definition name",
			nil,
			`definition someTenant/fo {}`,
			"parse error in `invalid definition name`, line 1, column 1: error in object definition someTenant/fo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
			[]SchemaDefinition{},
		},
		{
			"invalid relation name",
			nil,
			`definition some_tenant/foos {
				relation ab: some_tenant/foos
			}`,
			"parse error in `invalid relation name`, line 2, column 5: error in relation ab: invalid Relation.Name: value does not match regex pattern \"^[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
			[]SchemaDefinition{},
		},
		{
			"no implicit tenant with specified tenant on type ref",
			nil,
			`definition some_tenant/foos {
				relation somerel: some_tenant/bars
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("some_tenant/foos",
					namespace.MustRelation(
						"somerel",
						nil,
						namespace.AllowedRelation("some_tenant/bars", "..."),
					),
				),
			},
		},
		{
			"doc comments",
			&someTenant,
			`/**
			  * user is a user
			  */
			definition user {}

			/**
			 * single is a thing
			 */
			definition single {
				/**
				 * some permission
				 */
				permission first = bars + bazs
			}`,
			"",
			[]SchemaDefinition{
				namespace.WithComment("sometenant/user", `/**
* user is a user
*/`),
				namespace.WithComment("sometenant/single", `/**
* single is a thing
*/`,
					namespace.MustRelationWithComment("first", `/**
* some permission
*/`,
						namespace.Union(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
				),
			},
		},
		{
			"duplicate definition",
			&someTenant,
			`definition foo {}
			definition foo {}`,
			"parse error in `duplicate definition`, line 2, column 4: found name reused between multiple definitions and/or caveats: sometenant/foo",
			[]SchemaDefinition{},
		},
		{
			"duplicate definitions across objects and caveats",
			&someTenant,
			`caveat foo(someParam int) {
				someParam == 42
			}
			definition foo {}`,
			"parse error in `duplicate definitions across objects and caveats`, line 4, column 4: found name reused between multiple definitions and/or caveats: sometenant/foo",
			[]SchemaDefinition{},
		},
		{
			"caveat missing parameters",
			&someTenant,
			`caveat foo() {
				someParam == 42
			}`,
			"parse error in `caveat missing parameters`, line 1, column 12: Unexpected token at root level: TokenTypeRightParen",
			[]SchemaDefinition{},
		},
		{
			"caveat missing expression",
			&someTenant,
			`caveat foo(someParam int) {
			}`,
			"Unexpected token at root level: TokenTypeRightBrace",
			[]SchemaDefinition{},
		},
		{
			"caveat invalid parameter type",
			&someTenant,
			`caveat foo(someParam foobar) {
				someParam == 42
			}`,
			"parse error in `caveat invalid parameter type`, line 1, column 12: invalid type for caveat parameter `someParam` on caveat `foo`: unknown type `foobar`",
			[]SchemaDefinition{},
		},
		{
			"caveat invalid parameter type",
			&someTenant,
			`caveat foo(someParam map<foobar>) {
				someParam == 42
			}`,
			"unknown type `foobar`",
			[]SchemaDefinition{},
		},
		{
			"caveat missing parameter",
			&someTenant,
			`caveat foo(someParam int) {
				anotherParam == 42
			}`,
			`ERROR: sometenant/foo:2:5: undeclared reference to 'anotherParam'`,
			[]SchemaDefinition{},
		},
		{
			"caveat missing parameter on a different line",
			&someTenant,
			`caveat foo(someParam int) {
				someParam == 42 &&
					anotherParam
			}`,
			`ERROR: sometenant/foo:3:6: undeclared reference to 'anotherParam'`,
			[]SchemaDefinition{},
		},
		{
			"caveat invalid expression type",
			&someTenant,
			`caveat foo(someParam int) {
				someParam
			}`,
			`caveat expression must result in a boolean value`,
			[]SchemaDefinition{},
		},
		{
			"caveat invalid expression",
			&someTenant,
			`caveat foo(someParam int) {
				someParam:{}
			}`,
			`ERROR: sometenant/foo:2:14: Syntax error: mismatched input ':'`,
			[]SchemaDefinition{},
		},
		{
			"caveat valid",
			&someTenant,
			`caveat foo(someParam int) {
				someParam == 42
			}`,
			``,
			[]SchemaDefinition{
				namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
					map[string]caveattypes.VariableType{
						"someParam": caveattypes.IntType,
					},
				), "sometenant/foo", "someParam == 42"),
			},
		},
		{
			"long caveat valid",
			&someTenant,
			`caveat foo(someParam int, anotherParam string, thirdParam list<int>) {
				someParam == 42 && someParam != 43 && someParam < 12 &&
				someParam > 56 && anotherParam == "hi there" && 42 in thirdParam
			}`,
			``,
			[]SchemaDefinition{
				namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
					map[string]caveattypes.VariableType{
						"someParam":    caveattypes.IntType,
						"anotherParam": caveattypes.StringType,
						"thirdParam":   caveattypes.MustListType(caveattypes.IntType),
					},
				), "sometenant/foo",
					`someParam == 42 && someParam != 43 && someParam < 12 && someParam > 56 
					&& anotherParam == "hi there" && 42 in thirdParam`),
			},
		},
		{
			"caveat IP example",
			&someTenant,
			`caveat has_allowed_ip(user_ip ipaddress) {
				!user_ip.in_cidr('1.2.3.0')
			}`,
			``,
			[]SchemaDefinition{
				namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
					map[string]caveattypes.VariableType{
						"user_ip": caveattypes.IPAddressType,
					},
				), "sometenant/has_allowed_ip",
					`!user_ip.in_cidr('1.2.3.0')`),
			},
		},
		{
			"caveat subtree example",
			&someTenant,
			`caveat something(someMap map<any>, anotherMap map<any>) {
				someMap.isSubtreeOf(anotherMap)
			}`,
			``,
			[]SchemaDefinition{
				namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
					map[string]caveattypes.VariableType{
						"someMap":    caveattypes.MustMapType(caveattypes.AnyType),
						"anotherMap": caveattypes.MustMapType(caveattypes.AnyType),
					},
				), "sometenant/something",
					`someMap.isSubtreeOf(anotherMap)`),
			},
		},
		{
			"union permission with multiple branches",
			&someTenant,
			`definition simple {
				permission foos = first + second + third + fourth
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.ComputedUserset("third"),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"union permission with multiple branches, some not union",
			&someTenant,
			`definition simple {
				permission foos = first + second + (foo - bar) + fourth
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.Rewrite(
								namespace.Exclusion(
									namespace.ComputedUserset("foo"),
									namespace.ComputedUserset("bar"),
								),
							),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"intersection permission with multiple branches",
			&someTenant,
			`definition simple {
				permission foos = first & second & third & fourth
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Intersection(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.ComputedUserset("third"),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"exclusion permission with multiple branches",
			&someTenant,
			`definition simple {
				permission foos = first - second - third - fourth
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Exclusion(
							namespace.Rewrite(
								namespace.Exclusion(
									namespace.Rewrite(
										namespace.Exclusion(
											namespace.ComputedUserset("first"),
											namespace.ComputedUserset("second"),
										),
									),
									namespace.ComputedUserset("third"),
								),
							),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"wrong tenant is not translated",
			&someTenant,
			`definition someothertenant/simple {
				permission foos = (first + second) + (third + fourth)
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("someothertenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.ComputedUserset("third"),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"multiple-segment tenant",
			&someTenant,
			`definition sometenant/some_team/simple {
				permission foos = (first + second) + (third + fourth)
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/some_team/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.ComputedUserset("third"),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"multiple levels of compressed nesting",
			&someTenant,
			`definition simple {
				permission foos = (first + second) + (third + fourth)
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.ComputedUserset("third"),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"multiple levels",
			&someTenant,
			`definition simple {
				permission foos = (first + second) + (middle & thing) + (third + fourth)
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.Rewrite(
								namespace.Intersection(
									namespace.ComputedUserset("middle"),
									namespace.ComputedUserset("thing"),
								),
							),
							namespace.ComputedUserset("third"),
							namespace.ComputedUserset("fourth"),
						),
					),
				),
			},
		},
		{
			"multiple reduction",
			&someTenant,
			`definition simple {
				permission foos = first + second + (fourth & (sixth - seventh) & fifth) + third
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foos",
						namespace.Union(
							namespace.ComputedUserset("first"),
							namespace.ComputedUserset("second"),
							namespace.Rewrite(
								namespace.Intersection(
									namespace.ComputedUserset("fourth"),
									namespace.Rewrite(
										namespace.Exclusion(
											namespace.ComputedUserset("sixth"),
											namespace.ComputedUserset("seventh"),
										),
									),
									namespace.ComputedUserset("fifth"),
								),
							),
							namespace.ComputedUserset("third"),
						),
					),
				),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			compiled, err := Compile(InputSchema{
				input.Source(test.name), test.input,
			}, test.implicitTenant)

			if test.expectedError != "" {
				require.Error(err)
				require.Contains(err.Error(), test.expectedError)
			} else {
				require.Nil(err)
				require.Equal(len(test.expectedProto), len(compiled.OrderedDefinitions))
				for index, def := range compiled.OrderedDefinitions {
					filterSourcePositions(def.ProtoReflect())
					expectedDef := test.expectedProto[index]

					if caveatDef, ok := def.(*core.CaveatDefinition); ok {
						expectedCaveatDef, ok := expectedDef.(*core.CaveatDefinition)
						require.True(ok, "definition is not a caveat def")
						require.Equal(expectedCaveatDef.Name, caveatDef.Name)
						require.Equal(len(expectedCaveatDef.ParameterTypes), len(caveatDef.ParameterTypes))

						for expectedParamName, expectedParam := range expectedCaveatDef.ParameterTypes {
							foundParam, ok := caveatDef.ParameterTypes[expectedParamName]
							require.True(ok, "missing parameter %s", expectedParamName)
							testutil.RequireProtoEqual(t, expectedParam, foundParam, "mismatch type for parameter %s", expectedParamName)
						}

						parameterTypes, err := caveattypes.DecodeParameterTypes(caveatDef.ParameterTypes)
						require.NoError(err)

						expectedDecoded, err := caveats.DeserializeCaveat(expectedCaveatDef.SerializedExpression, parameterTypes)
						require.NoError(err)

						foundDecoded, err := caveats.DeserializeCaveat(caveatDef.SerializedExpression, parameterTypes)
						require.NoError(err)

						expectedExprString, err := expectedDecoded.ExprString()
						require.NoError(err)

						foundExprString, err := foundDecoded.ExprString()
						require.NoError(err)

						require.Equal(expectedExprString, foundExprString)
					} else {
						testutil.RequireProtoEqual(t, test.expectedProto[index], def, "proto mismatch")
					}
				}
			}
		})
	}
}

func filterSourcePositions(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind {
			if fd.IsList() {
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					filterSourcePositions(l.Get(i).Message())
				}
			} else if fd.IsMap() {
				m := v.Map()
				m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
					filterSourcePositions(v.Message())
					return true
				})
			} else {
				if string(fd.Message().Name()) == "SourcePosition" {
					m.Clear(fd)
				} else {
					filterSourcePositions(v.Message())
				}
			}
		}
		return true
	})
}
