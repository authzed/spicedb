package compiler

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

var (
	withTenantPrefix = ObjectTypePrefix("sometenant")
	nilPrefix        = func(cfg *config) { cfg.objectTypePrefix = nil }
)

func TestCompile(t *testing.T) {
	t.Parallel()

	type compileTest struct {
		name          string
		objectPrefix  ObjectPrefixOption
		input         string
		expectedError string
		expectedProto []SchemaDefinition
	}

	tests := []compileTest{
		{
			"empty",
			withTenantPrefix,
			"",
			"",
			[]SchemaDefinition{},
		},
		{
			"parse error",
			withTenantPrefix,
			"foo",
			"parse error in `parse error`, line 1, column 1: Unexpected token at root level: TokenTypeIdentifier",
			[]SchemaDefinition{},
		},
		{
			"nested parse error",
			withTenantPrefix,
			`definition foo {
				relation something: rela | relb + relc
			}`,
			"parse error in `nested parse error`, line 2, column 37: Expected end of statement or definition, found: TokenTypePlus",
			[]SchemaDefinition{},
		},
		{
			"allows bypassing prefix requirement",
			AllowUnprefixedObjectType(),
			`definition def {}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("def"),
			},
		},
		{
			"empty def",
			withTenantPrefix,
			`definition def {}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/def"),
			},
		},
		{
			"simple def",
			withTenantPrefix,
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
			"simple partial",
			withTenantPrefix,
			`partial view_partial {
				relation user: user;
			}

			definition simple {
				...view_partial
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"more complex partial",
			withTenantPrefix,
			`
			definition user {}
			definition organization {}

			partial view_partial {
				relation user: user;
				permission view = user
			}

			definition resource {
				relation organization: organization
				permission manage = organization

				...view_partial
			}
			`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/user"),
				namespace.Namespace("sometenant/organization"),
				namespace.Namespace("sometenant/resource",
					namespace.MustRelation("organization", nil,
						namespace.AllowedRelation("sometenant/organization", "..."),
					),
					namespace.MustRelation("manage",
						namespace.Union(
							namespace.ComputedUserset("organization"),
						),
					),
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
					namespace.MustRelation("view",
						namespace.Union(
							namespace.ComputedUserset("user"),
						),
					),
				),
			},
		},
		{
			"partial defined after reference",
			withTenantPrefix,
			`definition simple {
				...view_partial
			}

			partial view_partial {
				relation user: user;
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"transitive partials",
			withTenantPrefix,
			`
			partial view_partial {
				relation user: user;
			}

			partial transitive_partial {
				...view_partial
			}

			definition simple {
				...view_partial
			}
			`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"transitive partials out of order",
			withTenantPrefix,
			`
			partial transitive_partial {
				...view_partial
			}

			partial view_partial {
				relation user: user;
			}

			definition simple {
				...view_partial
			}
			`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"transitive partials in reverse order",
			withTenantPrefix,
			`
			definition simple {
				...view_partial
			}

			partial transitive_partial {
				...view_partial
			}

			partial view_partial {
				relation user: user;
			}
			`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"forking transitive partials out of order",
			withTenantPrefix,
			`
			partial transitive_partial {
				...view_partial
				...group_partial
			}

			partial view_partial {
				relation user: user;
			}

			partial group_partial {
				relation group: group;
			}

			definition simple {
				...transitive_partial
			}
			`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("user", nil,
						namespace.AllowedRelation("sometenant/user", "..."),
					),
					namespace.MustRelation("group", nil,
						namespace.AllowedRelation("sometenant/group", "..."),
					),
				),
			},
		},
		{
			"circular reference in partials",
			withTenantPrefix,
			`
			partial one_partial {
				...another_partial
			}

			partial another_partial {
				...one_partial
			}

			definition simple {
				...one_partial
			}
			`,
			"could not resolve partials",
			[]SchemaDefinition{},
		},
		{
			"definition reference to nonexistent partial",
			withTenantPrefix,
			`
			definition simple {
				...some_partial
			}
			`,
			"could not find partial reference",
			[]SchemaDefinition{},
		},
		{
			"definition with same name as partial",
			withTenantPrefix,
			`
			partial simple {
				relation user: user
			}
			definition simple {
				...simple
			}
			`,
			"found definition with same name as existing partial",
			[]SchemaDefinition{},
		},
		{
			"caveat with same name as partial",
			withTenantPrefix,
			`
			caveat some_caveat(someparam int) { someparam == 42}

			partial some_caveat {
				relation user: user
			}
			definition simple {
				...some_caveat
			}
			`,
			"found caveat with same name as existing partial",
			[]SchemaDefinition{},
		},
		{
			"definition reference to another definition errors",
			withTenantPrefix,
			`
			definition some_definition {}

			definition simple {
				...some_definition
			}
			`,
			"could not find partial reference",
			[]SchemaDefinition{},
		},
		{
			"explicit relation",
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
			`
			caveat somecaveat(someparam int) { someparam == 42}
			definition simple {
				relation viewer: user with somecaveat
			}`,
			"",
			[]SchemaDefinition{
				namespace.MustCaveatDefinition(caveats.MustEnvForVariables(
					map[string]caveattypes.VariableType{
						"someparam": caveattypes.IntType,
					},
				), "sometenant/somecaveat", "someparam == 42"),
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveat("sometenant/user", "...",
							namespace.AllowedCaveat("sometenant/somecaveat")),
					),
				),
			},
		},
		{
			"relation with optional caveat",
			withTenantPrefix,
			`definition simple {
				relation viewer: user with somecaveat | user
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveat("sometenant/user", "...",
							namespace.AllowedCaveat("sometenant/somecaveat")),
						namespace.AllowedRelation("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"relation with multiple caveats",
			withTenantPrefix,
			`definition simple {
				relation viewer: user with somecaveat | user | team#member with anothercaveat
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveat("sometenant/user", "...",
							namespace.AllowedCaveat("sometenant/somecaveat")),
						namespace.AllowedRelation("sometenant/user", "..."),
						namespace.AllowedRelationWithCaveat("sometenant/team", "member",
							namespace.AllowedCaveat("sometenant/anothercaveat")),
					),
				),
			},
		},
		{
			"simple permission",
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			nilPrefix,
			`definition foos {}`,
			"parse error in `no implicit tenant with unspecified tenant`, line 1, column 1: found reference `foos` without prefix",
			[]SchemaDefinition{},
		},
		{
			"no implicit tenant with specified tenant",
			nilPrefix,
			`definition some_tenant/foos {}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("some_tenant/foos"),
			},
		},
		{
			"no implicit tenant with unspecified tenant on type ref",
			nilPrefix,
			`definition some_tenant/foo {
				relation somerel: bars
			}`,
			"parse error in `no implicit tenant with unspecified tenant on type ref`, line 2, column 23: found reference `bars` without prefix",
			[]SchemaDefinition{},
		},
		{
			"invalid definition name",
			nilPrefix,
			`definition someTenant/fo {}`,
			"parse error in `invalid definition name`, line 1, column 1: error in object definition someTenant/fo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
			[]SchemaDefinition{},
		},
		{
			"invalid relation name",
			nilPrefix,
			`definition some_tenant/foos {
				relation ab: some_tenant/foos
			}`,
			"parse error in `invalid relation name`, line 2, column 5: error in relation ab: invalid Relation.Name: value does not match regex pattern \"^[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
			[]SchemaDefinition{},
		},
		{
			"no implicit tenant with specified tenant on type ref",
			nilPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
			`definition foo {}
			definition foo {}`,
			"parse error in `duplicate definition`, line 2, column 4: found name reused between multiple definitions and/or caveats: sometenant/foo",
			[]SchemaDefinition{},
		},
		{
			"duplicate definitions across objects and caveats",
			withTenantPrefix,
			`caveat foo(someParam int) {
				someParam == 42
			}
			definition foo {}`,
			"parse error in `duplicate definitions across objects and caveats`, line 4, column 4: found name reused between multiple definitions and/or caveats: sometenant/foo",
			[]SchemaDefinition{},
		},
		{
			"caveat missing parameters",
			withTenantPrefix,
			`caveat foo() {
				someParam == 42
			}`,
			"parse error in `caveat missing parameters`, line 1, column 12: Unexpected token at root level: TokenTypeRightParen",
			[]SchemaDefinition{},
		},
		{
			"caveat missing expression",
			withTenantPrefix,
			`caveat foo(someParam int) {
			}`,
			"Unexpected token at root level: TokenTypeRightBrace",
			[]SchemaDefinition{},
		},
		{
			"caveat invalid parameter type",
			withTenantPrefix,
			`caveat foo(someParam foobar) {
				someParam == 42
			}`,
			"parse error in `caveat invalid parameter type`, line 1, column 12: invalid type for caveat parameter `someParam` on caveat `foo`: unknown type `foobar`",
			[]SchemaDefinition{},
		},
		{
			"caveat invalid parameter type",
			withTenantPrefix,
			`caveat foo(someParam map<foobar>) {
				someParam == 42
			}`,
			"unknown type `foobar`",
			[]SchemaDefinition{},
		},
		{
			"caveat missing parameter",
			withTenantPrefix,
			`caveat foo(someParam int) {
				anotherParam == 42
			}`,
			`ERROR: sometenant/foo:2:5: undeclared reference to 'anotherParam'`,
			[]SchemaDefinition{},
		},
		{
			"caveat missing parameter on a different line",
			withTenantPrefix,
			`caveat foo(someParam int) {
				someParam == 42 &&
					anotherParam
			}`,
			`ERROR: sometenant/foo:3:6: undeclared reference to 'anotherParam'`,
			[]SchemaDefinition{},
		},
		{
			"caveat invalid expression type",
			withTenantPrefix,
			`caveat foo(someParam int) {
				someParam
			}`,
			`caveat expression must result in a boolean value`,
			[]SchemaDefinition{},
		},
		{
			"caveat invalid expression",
			withTenantPrefix,
			`caveat foo(someParam int) {
				someParam:{}
			}`,
			`ERROR: sometenant/foo:2:14: Syntax error: mismatched input ':'`,
			[]SchemaDefinition{},
		},
		{
			"caveat valid",
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
			withTenantPrefix,
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
		{
			"any arrow",
			withTenantPrefix,
			`definition simple {
				permission foo = bar.any(baz)
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foo",
						namespace.Union(
							namespace.MustFunctionedTupleToUserset("bar", "any", "baz"),
						),
					),
				),
			},
		},
		{
			"all arrow",
			withTenantPrefix,
			`definition simple {
				permission foo = bar.all(baz)
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("foo",
						namespace.Union(
							namespace.MustFunctionedTupleToUserset("bar", "all", "baz"),
						),
					),
				),
			},
		},
		{
			"relation with expiration trait",
			withTenantPrefix,
			`use expiration

			definition simple {
				relation viewer: user with expiration
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithExpiration("sometenant/user", "..."),
					),
				),
			},
		},
		{
			"duplicate use pragmas",
			withTenantPrefix,
			`
			use expiration
			use expiration

			definition simple {
				relation viewer: user with expiration
			}`,
			`found duplicate use flag`,
			[]SchemaDefinition{},
		},
		{
			"expiration use without use expiration",
			withTenantPrefix,
			`
			definition simple {
				relation viewer: user with expiration
			}`,
			`expiration flag is not enabled`,
			[]SchemaDefinition{},
		},
		{
			"relation with expiration trait and caveat",
			withTenantPrefix,
			`use expiration

			definition simple {
				relation viewer: user with somecaveat and expiration
			}`,
			"",
			[]SchemaDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.MustRelation("viewer", nil,
						namespace.AllowedRelationWithCaveatAndExpiration("sometenant/user", "...", namespace.AllowedCaveat("sometenant/somecaveat")),
					),
				),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			compiled, err := Compile(InputSchema{
				input.Source(test.name), test.input,
			}, test.objectPrefix)

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

func TestSkipValidation(t *testing.T) {
	t.Parallel()

	_, err := Compile(InputSchema{"test", `definition a/def {}`}, AllowUnprefixedObjectType())
	require.Error(t, err)

	_, err = Compile(InputSchema{"test", `definition a/def {}`}, AllowUnprefixedObjectType(), SkipValidation())
	require.NoError(t, err)
}

func TestSuperLargeCaveatCompile(t *testing.T) {
	t.Parallel()

	b, err := os.ReadFile("../parser/tests/superlarge.zed")
	if err != nil {
		panic(err)
	}

	compiled, err := Compile(InputSchema{
		"superlarge", string(b),
	}, AllowUnprefixedObjectType())
	require.NoError(t, err)
	require.Equal(t, 29, len(compiled.ObjectDefinitions))
	require.Equal(t, 1, len(compiled.CaveatDefinitions))
}
