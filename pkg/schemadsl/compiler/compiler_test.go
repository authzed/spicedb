package compiler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/namespace"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
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
				relation something: a | b + c	
			}`,
			"parse error in `nested parse error`, line 2, column 31: Expected end of statement or definition, found: TokenTypePlus",
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
				relation foo: bar;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("sometenant/bar", "..."),
					),
				),
			},
		},
		{
			"explicit relation",
			&someTenant,
			`definition simple {
				relation foo: bar#meh;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("sometenant/bar", "meh"),
					),
				),
			},
		},
		{
			"cross tenant relation",
			&someTenant,
			`definition simple {
				relation foo: anothertenant/bar#meh;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("anothertenant/bar", "meh"),
					),
				),
			},
		},
		{
			"multiple relations",
			&someTenant,
			`definition simple {
				relation foo: bar#meh;
				relation hi: there | world;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("sometenant/bar", "meh"),
					),
					namespace.Relation("hi", nil,
						namespace.RelationReference("sometenant/there", "..."),
						namespace.RelationReference("sometenant/world", "..."),
					),
				),
			},
		},
		{
			"simple permission",
			&someTenant,
			`definition simple {
				permission foo = bar;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo",
						namespace.Union(
							namespace.ComputedUserset("bar"),
						),
					),
				),
			},
		},
		{
			"union permission",
			&someTenant,
			`definition simple {
				permission foo = bar + baz;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo",
						namespace.Union(
							namespace.ComputedUserset("bar"),
							namespace.ComputedUserset("baz"),
						),
					),
				),
			},
		},
		{
			"intersection permission",
			&someTenant,
			`definition simple {
				permission foo = bar & baz;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo",
						namespace.Intersection(
							namespace.ComputedUserset("bar"),
							namespace.ComputedUserset("baz"),
						),
					),
				),
			},
		},
		{
			"exclusion permission",
			&someTenant,
			`definition simple {
				permission foo = bar - baz;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo",
						namespace.Exclusion(
							namespace.ComputedUserset("bar"),
							namespace.ComputedUserset("baz"),
						),
					),
				),
			},
		},
		{
			"multi-union permission",
			&someTenant,
			`definition simple {
				permission foo = bar + baz + meh;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo",
						namespace.Union(
							namespace.Rewrite(
								namespace.Union(
									namespace.ComputedUserset("bar"),
									namespace.ComputedUserset("baz"),
								),
							),
							namespace.ComputedUserset("meh"),
						),
					),
				),
			},
		},
		{
			"complex permission",
			&someTenant,
			`definition complex {
				permission foo = bar + baz - meh;
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/complex",
					namespace.Relation("foo",
						namespace.Exclusion(
							namespace.Rewrite(
								namespace.Union(
									namespace.ComputedUserset("bar"),
									namespace.ComputedUserset("baz"),
								),
							),
							namespace.ComputedUserset("meh"),
						),
					),
				),
			},
		},
		{
			"complex parens permission",
			&someTenant,
			`definition complex {
				permission foo = bar + (baz - meh);
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/complex",
					namespace.Relation("foo",
						namespace.Union(
							namespace.ComputedUserset("bar"),
							namespace.Rewrite(
								namespace.Exclusion(
									namespace.ComputedUserset("baz"),
									namespace.ComputedUserset("meh"),
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
				permission foo = bar->baz
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

		{
			"multiarrow permission",
			&someTenant,
			`definition arrowed {
				relation a: something;
				permission foo = a->b->c
			}`,
			"Nested arrows not yet supported",
			[]*v0.NamespaceDefinition{},
		},

		/*
			 * TODO: uncomment once supported and remove the test above
			{
				"multiarrow permission",
				`definition arrowed {
					relation a: something;
					permission foo = a->b->c
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
				permission foo = ((a->b) + c) - d
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/expressioned",
					namespace.Relation("foo",
						namespace.Exclusion(
							namespace.Rewrite(
								namespace.Union(
									namespace.TupleToUserset("a", "b"),
									namespace.ComputedUserset("c"),
								),
							),
							namespace.ComputedUserset("d"),
						),
					),
				),
			},
		},
		{
			"multiple permission",
			&someTenant,
			`definition multiple {
				permission first = bar + baz
				permission second = bar - baz
				permission third = bar & baz
				permission fourth = bar->baz
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("sometenant/multiple",
					namespace.Relation("first",
						namespace.Union(
							namespace.ComputedUserset("bar"),
							namespace.ComputedUserset("baz"),
						),
					),
					namespace.Relation("second",
						namespace.Exclusion(
							namespace.ComputedUserset("bar"),
							namespace.ComputedUserset("baz"),
						),
					),
					namespace.Relation("third",
						namespace.Intersection(
							namespace.ComputedUserset("bar"),
							namespace.ComputedUserset("baz"),
						),
					),
					namespace.Relation("fourth",
						namespace.Union(
							namespace.TupleToUserset("bar", "baz"),
						),
					),
				),
			},
		},
		{
			"no implicit tenant with unspecified tenant",
			nil,
			`definition foo {}`,
			"found reference `foo` without prefix",
			[]*v0.NamespaceDefinition{},
		},
		{
			"no implicit tenant with specified tenant",
			nil,
			`definition someTenant/foo {}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("someTenant/foo"),
			},
		},
		{
			"no implicit tenant with unspecified tenant on type ref",
			nil,
			`definition someTenant/foo {
				relation somerel: bar
			}`,
			"found reference `bar` without prefix",
			[]*v0.NamespaceDefinition{},
		},
		{
			"no implicit tenant with specified tenant on type ref",
			nil,
			`definition someTenant/foo {
				relation somerel: someTenant/bar
			}`,
			"",
			[]*v0.NamespaceDefinition{
				namespace.Namespace("someTenant/foo",
					namespace.Relation(
						"somerel",
						nil,
						namespace.RelationReference("someTenant/bar", "..."),
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
