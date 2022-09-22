package compiler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

var someTenant = "sometenant"

func TestCompile(t *testing.T) {
	type compileTest struct {
		name           string
		implicitTenant *string
		input          string
		expectedError  string
		expectedProto  []*core.NamespaceDefinition
	}

	tests := []compileTest{
		{
			"empty",
			&someTenant,
			"",
			"",
			[]*core.NamespaceDefinition{},
		},
		{
			"parse error",
			&someTenant,
			"foo",
			"parse error in `parse error`, line 1, column 1: Unexpected token at root level: TokenTypeIdentifier",
			[]*core.NamespaceDefinition{},
		},
		{
			"nested parse error",
			&someTenant,
			`definition foo {
				relation something: rela | relb + relc	
			}`,
			"parse error in `nested parse error`, line 2, column 37: Expected end of statement or definition, found: TokenTypePlus",
			[]*core.NamespaceDefinition{},
		},
		{
			"empty def",
			&someTenant,
			`definition def {}`,
			"",
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foo", nil,
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{},
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
				[]*core.NamespaceDefinition{
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
				permission foos = ((arel->brel) + nil) - drel
			}`,
			"",
			[]*core.NamespaceDefinition{
				namespace.Namespace("sometenant/expressioned",
					namespace.Relation("foos",
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
			[]*core.NamespaceDefinition{
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
			"permission with nil",
			&someTenant,
			`definition simple {
				permission foos = aaaa + nil + bbbb;
			}`,
			"",
			[]*core.NamespaceDefinition{
				namespace.Namespace("sometenant/simple",
					namespace.Relation("foos",
						namespace.Union(
							namespace.Rewrite(
								namespace.Union(
									namespace.ComputedUserset("aaaa"),
									namespace.Nil(),
								),
							),
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
			[]*core.NamespaceDefinition{},
		},
		{
			"no implicit tenant with specified tenant",
			nil,
			`definition some_tenant/foos {}`,
			"",
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{},
		},
		{
			"invalid definition name",
			nil,
			`definition someTenant/fo {}`,
			"parse error in `invalid definition name`, line 1, column 1: error in object definition someTenant/fo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)?[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
			[]*core.NamespaceDefinition{},
		},
		{
			"invalid relation name",
			nil,
			`definition some_tenant/foos {
				relation ab: some_tenant/foos
			}`,
			"parse error in `invalid relation name`, line 2, column 5: error in relation ab: invalid Relation.Name: value does not match regex pattern \"^[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
			[]*core.NamespaceDefinition{},
		},
		{
			"no implicit tenant with specified tenant on type ref",
			nil,
			`definition some_tenant/foos {
				relation somerel: some_tenant/bars
			}`,
			"",
			[]*core.NamespaceDefinition{
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
			[]*core.NamespaceDefinition{
				namespace.WithComment("sometenant/user", `/**
* user is a user
*/`),
				namespace.WithComment("sometenant/single", `/**
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
		{
			"duplicate definition",
			&someTenant,
			`definition foo {}
			definition foo {}`,
			"parse error in `duplicate definition`, line 2, column 4: duplicate definition: sometenant/foo",
			[]*core.NamespaceDefinition{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			defs, err := Compile([]InputSchema{
				{input.Source(test.name), test.input},
			}, test.implicitTenant)

			if test.expectedError != "" {
				require.Error(err)
				require.Equal(test.expectedError, err.Error())
			} else {
				require.Nil(err)
				require.Equal(len(test.expectedProto), len(defs))
				for index, def := range defs {
					filterSourcePositions(def.ProtoReflect())
					require.True(proto.Equal(test.expectedProto[index], def))
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
