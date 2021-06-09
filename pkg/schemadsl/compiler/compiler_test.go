package compiler

import (
	"testing"

	"github.com/authzed/spicedb/pkg/namespace"
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	type compileTest struct {
		name          string
		input         string
		expectedError string
		expectedProto []*pb.NamespaceDefinition
	}

	tests := []compileTest{
		{
			"empty",
			"",
			"",
			[]*pb.NamespaceDefinition{},
		},
		{
			"parse error",
			"foo",
			"parse error in `parse error`, line 1, column 1: Unexpected token at root level: TokenTypeIdentifier",
			[]*pb.NamespaceDefinition{},
		},
		{
			"nested parse error",
			`definition foo {
				relation something: a | b + c	
			}`,
			"parse error in `nested parse error`, line 2, column 31: Expected end of statement or definition, found: TokenTypePlus",
			[]*pb.NamespaceDefinition{},
		},
		{
			"empty def",
			`definition empty {}`,
			"",
			[]*pb.NamespaceDefinition{
				namespace.Namespace("sometenant/empty"),
			},
		},
		{
			"simple def",
			`definition simple {
				relation foo: bar;
			}`,
			"",
			[]*pb.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("sometenant/bar", "..."),
					),
				),
			},
		},
		{
			"explicit relation",
			`definition simple {
				relation foo: bar#meh;
			}`,
			"",
			[]*pb.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("sometenant/bar", "meh"),
					),
				),
			},
		},
		{
			"cross tenant relation",
			`definition simple {
				relation foo: anothertenant/bar#meh;
			}`,
			"",
			[]*pb.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
						namespace.RelationReference("anothertenant/bar", "meh"),
					),
				),
			},
		},
		{
			"multiple relations",
			`definition simple {
				relation foo: bar#meh;
				relation hi: there | world;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition simple {
				permission foo = bar;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition simple {
				permission foo = bar + baz;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition simple {
				permission foo = bar & baz;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition simple {
				permission foo = bar - baz;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition simple {
				permission foo = bar + baz + meh;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition complex {
				permission foo = bar + baz - meh;
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition complex {
				permission foo = bar + (baz - meh);
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition arrowed {
				permission foo = bar->baz
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition arrowed {
				relation a: something;
				permission foo = a->b->c
			}`,
			"Nested arrows not yet supported",
			[]*pb.NamespaceDefinition{},
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
				[]*pb.NamespaceDefinition{
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
			`definition expressioned {
				permission foo = ((a->b) + c) - d
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
			`definition multiple {
				permission first = bar + baz
				permission second = bar - baz
				permission third = bar & baz
				permission fourth = bar->baz
			}`,
			"",
			[]*pb.NamespaceDefinition{
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			defs, err := Compile([]InputSchema{
				InputSchema{input.InputSource(test.name), test.input},
			}, "sometenant")

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
