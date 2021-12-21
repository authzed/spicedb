package compiler

import (
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

var someTenant = "sometenant"

func TestCompile(t *testing.T) {
	type compileTest struct {
		name           string
		implicitTenant *string
		input          string
		expectedError  string
		expectedProto  []*v0.NamespaceDefinition
	}

	tests := []compileTest{
		{
			"empty",
			&someTenant,
			"",
			"",
			[]*v0.NamespaceDefinition{},
		},
		{
			"parse error",
			&someTenant,
			"foo",
			"parse error in `parse error`, line 1, column 1: Unexpected token at root level: TokenTypeIdentifier",
			[]*v0.NamespaceDefinition{},
		},
		{
			"nested parse error",
			&someTenant,
			`definition foo {
				relation something: rela | relb + relc	
			}`,
			"parse error in `nested parse error`, line 2, column 37: Expected end of statement or definition, found: TokenTypePlus",
			[]*v0.NamespaceDefinition{},
		},
		{
			"empty def",
			&someTenant,
			`definition empty {}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/empty"),
			},
		},
		{
			"simple def",
			&someTenant,
			`definition simple {
				relation foos: bars;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos", nil,
						namespace.AllowedRelation("sometenant/bars", "..."),
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos", nil,
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos", nil,
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos", nil,
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos", nil,
						namespace.AllowedRelation("sometenant/bars", "mehs"),
					),
					namespace.Relation("hello", nil,
						namespace.AllowedRelation("sometenant/there", "..."),
						namespace.AllowedRelation("sometenant/world", "..."),
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos",
						namespace.Union(
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
			"complex permission",
			&someTenant,
			`definition complex {
				permission foos = bars + bazs - mehs;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/complex",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/complex",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/arrowed",
					namespace.Relation("foos",
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
			[]*v0.NamespaceDefinition{},
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
				[]*v0.NamespaceDefinition{
					namespace.Namespace("sometenant/arrowed",
						namespace.Relation("foo",
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
				permission foos = ((arel->brel) + crel) - drel
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/expressioned",
					namespace.Relation("foos",
						namespace.Exclusion(
							namespace.Rewrite(
								namespace.Union(
									namespace.TupleToUserset("arel", "brel"),
									namespace.ComputedUserset("crel"),
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
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/multiple",
					namespace.Relation("first",
						namespace.Union(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
					namespace.Relation("second",
						namespace.Exclusion(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
					namespace.Relation("third",
						namespace.Intersection(
							namespace.ComputedUserset("bars"),
							namespace.ComputedUserset("bazs"),
						),
					),
					namespace.Relation("fourth",
						namespace.Union(
							namespace.TupleToUserset("bars", "bazs"),
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
			[]*v0.NamespaceDefinition{},
		},
		{
			"no implicit tenant with specified tenant",
			nil,
			`definition some_tenant/foos {}`,
			"",
			[]*v0.NamespaceDefinition{
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
			[]*v0.NamespaceDefinition{},
		},
		{
			"invalid definition name",
			nil,
			`definition someTenant/foo {}`,
			"parse error in `invalid definition name`, line 1, column 1: error in object definition someTenant/foo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{2,62}[a-z0-9]/)?[a-z][a-z0-9_]{2,62}[a-z0-9]$\"",
			[]*v0.NamespaceDefinition{},
		},
		{
			"invalid relation name",
			nil,
			`definition some_tenant/foos {
				relation bar: some_tenant/foos
			}`,
			"parse error in `invalid relation name`, line 2, column 5: error in relation bar: invalid Relation.Name: value does not match regex pattern \"^[a-z][a-z0-9_]{2,62}[a-z0-9]$\"",
			[]*v0.NamespaceDefinition{},
		},
		{
			"no implicit tenant with specified tenant on type ref",
			nil,
			`definition some_tenant/foos {
				relation somerel: some_tenant/bars
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("some_tenant/foos",
					namespace.Relation(
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
			[]*v0.NamespaceDefinition{
				namespace.NamespaceWithComment("sometenant/user", `/**
* user is a user
*/`),
				namespace.NamespaceWithComment("sometenant/single", `/**
* single is a thing
*/`,
					namespace.RelationWithComment("first", `/**
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			defs, err := Compile([]InputSchema{
				{input.InputSource(test.name), test.input},
			}, test.implicitTenant)

			if test.expectedError != "" {
				require.Error(err)
				require.Equal(test.expectedError, err.Error())
			} else {
				require.Nil(err)
				require.Equal(test.expectedProto, defs)
			}
		})
	}
}
