//go:build wasm
// +build wasm

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

type editCheckResult struct {
	Relationship  *core.RelationTuple
	IsMember      bool
	Error         *devinterface.DeveloperError
	IsConditional bool
}

func TestCheckOperation(t *testing.T) {
	type testCase struct {
		name              string
		schema            string
		relationships     []*core.RelationTuple
		checkRelationship *core.RelationTuple
		caveatContext     map[string]any
		expectedError     *devinterface.DeveloperError
		expectedResult    *editCheckResult
	}

	tests := []testCase{
		{
			"invalid namespace",
			`definition foo {
				relation bar:
			}`,
			[]*core.RelationTuple{},
			tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "Expected identifier, found token TokenTypeRightBrace",
				Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
				Source:  devinterface.DeveloperError_SCHEMA,
				Line:    3,
				Column:  4,
				Context: "}",
			},
			nil,
		},
		{
			"invalid namespace name",
			`definition fo {}`,
			[]*core.RelationTuple{},
			tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "error in object definition fo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)?[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
				Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
				Source:  devinterface.DeveloperError_SCHEMA,
				Line:    1,
				Column:  1,
			},
			nil,
		},
		{
			"invalid shared name",
			`definition user {}
			
			definition resource {
				relation writer: user
				 permission writer = writer
			}`,
			[]*core.RelationTuple{},
			tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "found duplicate relation/permission name `writer` under definition `resource`",
				Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
				Source:  devinterface.DeveloperError_SCHEMA,
				Line:    5,
				Column:  6,
				Context: "writer",
			},
			nil,
		},
		{
			"invalid check",
			`
				definition user {}
				definition somenamespace {
					relation somerel: user
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			},
			tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
				Error: &devinterface.DeveloperError{
					Message: "relation/permission `anotherrel` not found under definition `somenamespace`",
					Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
					Source:  devinterface.DeveloperError_CHECK_WATCH,
					Context: "somenamespace:someobj#anotherrel@user:foo",
				},
			},
		},
		{
			"valid check",
			`
				definition user {}
				definition somenamespace {
					relation somerel: user
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			},
			tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				IsMember:     true,
			},
		},
		{
			"valid negative check",
			`
				definition user {}
				definition somenamespace {
					relation somerel: user
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			},
			tuple.MustParse("somenamespace:someobj#somerel@user:bar"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#somerel@user:bar"),
				IsMember:     false,
			},
		},
		{
			"valid wildcard check",
			`
				definition user {}
				definition somenamespace {
					relation somerel: user | user:*
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:*"),
			},
			tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				IsMember:     true,
			},
		},
		{
			"valid nil check",
			`
				definition user {}
				definition somenamespace {
					permission empty = nil
				}
			`,
			[]*core.RelationTuple{},
			tuple.MustParse("somenamespace:someobj#empty@user:foo"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#empty@user:foo"),
				IsMember:     false,
			},
		},
		{
			"recursive check",
			`
				definition user {}
				definition document {
					relation viewer: user | document#viewer
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:someobj#viewer@document:someobj#viewer"),
			},
			tuple.MustParse("document:someobj#viewer@user:foo"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("document:someobj#viewer@user:foo"),
				Error: &devinterface.DeveloperError{
					Message: "max depth exceeded: this usually indicates a recursive or too deep data dependency",
					Kind:    devinterface.DeveloperError_MAXIMUM_RECURSION,
					Source:  devinterface.DeveloperError_CHECK_WATCH,
					Context: "document:someobj#viewer@user:foo",
				},
			},
		},
		{
			"valid caveated check to negative",
			`
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}

				definition user {}
				definition somenamespace {
					relation somerel: user with somecaveat
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo[somecaveat]"),
			},
			tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			map[string]any{"somecondition": 41},
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				IsMember:     false,
			},
		},
		{
			"valid caveated check to positive",
			`
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}

				definition user {}
				definition somenamespace {
					relation somerel: user with somecaveat
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo[somecaveat]"),
			},
			tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			map[string]any{"somecondition": 42},
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				IsMember:     true,
			},
		},
		{
			"valid caveated check to conditional",
			`
				caveat somecaveat(somecondition int) {
					somecondition == 42
				}

				definition user {}
				definition somenamespace {
					relation somerel: user with somecaveat
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo[somecaveat]"),
			},
			tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			map[string]any{"anothercondition": 42},
			nil,
			&editCheckResult{
				Relationship:  tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				IsMember:      false,
				IsConditional: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var caveatContext *structpb.Struct
			if len(tc.caveatContext) > 0 {
				cc, err := structpb.NewStruct(tc.caveatContext)
				require.NoError(t, err)
				caveatContext = cc
			}

			response := run(t, &devinterface.DeveloperRequest{
				Context: &devinterface.RequestContext{
					Schema:        tc.schema,
					Relationships: tc.relationships,
				},
				Operations: []*devinterface.Operation{
					{
						CheckParameters: &devinterface.CheckOperationParameters{
							Resource:      tc.checkRelationship.ResourceAndRelation,
							Subject:       tc.checkRelationship.Subject,
							CaveatContext: caveatContext,
						},
					},
				},
			})

			if tc.expectedError != nil {
				require.NotNil(t, response.GetDeveloperErrors())
				require.Equal(t, 1, len(response.GetDeveloperErrors().InputErrors))
				testutil.RequireProtoEqual(t, tc.expectedError, response.GetDeveloperErrors().InputErrors[0], "found mismatching error")
			} else {
				require.Equal(t, "", response.GetInternalError())
				require.Nil(t, response.GetDeveloperErrors())

				checkResult := response.GetOperationsResults().Results[0].GetCheckResult()
				require.NotNil(t, checkResult)

				if tc.expectedResult.Error != nil {
					testutil.RequireProtoEqual(t, tc.expectedResult.Error, checkResult.CheckError, "found mismatching error")
				} else {
					require.Nil(t, checkResult.CheckError)
					require.Equal(t, tc.expectedResult.IsMember, checkResult.Membership == devinterface.CheckOperationsResult_MEMBER)
					require.Equal(t, tc.expectedResult.IsConditional, checkResult.Membership == devinterface.CheckOperationsResult_CAVEATED_MEMBER)
				}
			}
		})
	}
}

func TestFormatSchemaOperation(t *testing.T) {
	require := require.New(t)
	response := run(t, &devinterface.DeveloperRequest{
		Context: &devinterface.RequestContext{
			Schema: "/** hi there */definition foos {} definition bars{}",
		},
		Operations: []*devinterface.Operation{
			{
				FormatSchemaParameters: &devinterface.FormatSchemaParameters{},
			},
		},
	})

	formatResult := response.GetOperationsResults().Results[0].GetFormatSchemaResult()
	require.Equal("/** hi there */\ndefinition foos {}\n\ndefinition bars {}", formatResult.FormattedSchema)
}

func TestRunAssertionsAndValidationOperations(t *testing.T) {
	type testCase struct {
		name                   string
		schema                 string
		relationships          []*core.RelationTuple
		validationYaml         string
		assertionsYaml         string
		expectedError          *devinterface.DeveloperError
		expectedValidationYaml string
	}

	tests := []testCase{
		{
			"valid namespace",
			`definition somenamespace {}`,
			[]*core.RelationTuple{},
			"",
			"",
			nil,
			"{}\n",
		},
		{
			"invalid validation yaml",
			`definition somenamespace {}`,
			[]*core.RelationTuple{},
			`asdkjhgasd`,
			"",
			&devinterface.DeveloperError{
				Message: "unexpected value `asdkjhg`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "asdkjhg",
				Line:    1,
			},
			"",
		},
		{
			"invalid assertions yaml",
			`definition somenamespace {}`,
			[]*core.RelationTuple{},
			"",
			`asdhasjdkhjasd`,
			&devinterface.DeveloperError{
				Message: "unexpected value `asdhasj`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_ASSERTION,
				Context: "asdhasj",
				Line:    1,
			},
			"",
		},
		{
			"assertions yaml with garbage",
			`definition somenamespace {}`,
			[]*core.RelationTuple{},
			"",
			`assertTrue:
- document:firstdoc#view@user:tom
- document:firstdoc#view@user:fred
- document:seconddoc#view@user:tom
assertFalse: garbage
- document:seconddoc#view@user:fred`,
			&devinterface.DeveloperError{
				Message: "did not find expected key",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_ASSERTION,
				Line:    5,
			},
			"",
		},
		{
			"assertions yaml with indented garbage",
			`definition somenamespace {}`,
			[]*core.RelationTuple{},
			"",
			`assertTrue:
  - document:firstdoc#view@user:tom
  - document:firstdoc#view@user:fred
  - document:seconddoc#view@user:tom
assertFalse: garbage
  - document:seconddoc#view@user:fred`,
			&devinterface.DeveloperError{
				Message: "unexpected value `garbage`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_ASSERTION,
				Line:    5,
				Column:  0,
				Context: "garbage",
			},
			"",
		},
		{
			"invalid assertions true yaml",
			`definition somenamespace {}`,
			[]*core.RelationTuple{},
			"",
			`assertTrue:
- something`,
			&devinterface.DeveloperError{
				Message: "error parsing relationship in assertion `something`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_ASSERTION,
				Line:    2,
				Column:  3,
				Context: "something",
			},
			"",
		},
		{
			"assertion true failure",
			`
				definition user {}
				definition document {
					relation viewer: user
				}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#viewer@user:jimmy")},
			"",
			`assertTrue:
- document:somedoc#viewer@user:jake`,
			&devinterface.DeveloperError{
				Message: "Expected relation or permission document:somedoc#viewer@user:jake to exist",
				Kind:    devinterface.DeveloperError_ASSERTION_FAILED,
				Source:  devinterface.DeveloperError_ASSERTION,
				Context: "document:somedoc#viewer@user:jake",
				Line:    2,
				Column:  3,
			},
			"{}\n",
		},
		{
			"assertion false failure",
			`
				definition user {}
				definition document {
					relation viewer: user
				}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#viewer@user:jimmy")},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "Expected relation or permission document:somedoc#viewer@user:jimmy to not exist",
				Kind:    devinterface.DeveloperError_ASSERTION_FAILED,
				Source:  devinterface.DeveloperError_ASSERTION,
				Context: "document:somedoc#viewer@user:jimmy",
				Line:    2,
				Column:  3,
			},
			"{}\n",
		},
		{
			"assertion invalid caveated relation",
			`
				definition user {}
				definition document {}
			`,
			[]*core.RelationTuple{},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy[somecaveat]`,
			&devinterface.DeveloperError{
				Message: "cannot specify a caveat on an assertion: `document:somedoc#viewer@user:jimmy[somecaveat]`",
				Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
				Source:  devinterface.DeveloperError_ASSERTION,
				Context: "document:somedoc#viewer@user:jimmy[somecaveat]",
				Line:    2,
				Column:  3,
			},
			"{}\n",
		},
		{
			"assertion invalid relation",
			`
				definition user {}
				definition document {}
			`,
			[]*core.RelationTuple{},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "relation/permission `viewer` not found under definition `document`",
				Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
				Source:  devinterface.DeveloperError_ASSERTION,
				Context: "document:somedoc#viewer@user:jimmy",
				Line:    2,
				Column:  3,
			},
			"{}\n",
		},
		{
			"missing subject",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, subject `user:jimmy` found but missing from specified",
				Kind:    devinterface.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "document:somedoc#view",
				Line:    1,
				Column:  1,
			},
			`document:somedoc#view:
- '[user:jimmy] is <document:somedoc#writer>'
`,
		},
		{
			"extra subject",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":
- "[user:jimmy] is <document:somedoc#writer>"
- "[user:jake] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, missing expected subject `user:jake`",
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "[user:jake] is <document:somedoc#viewer>",
				Line:    3,
				Column:  3,
			},
			`document:somedoc#view:
- '[user:jimmy] is <document:somedoc#writer>'
`,
		},
		{
			"parse error in validation",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":
- "[user] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "invalid subject: `user`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "user",
				Line:    2,
				Column:  3,
			},
			``,
		},
		{
			"parse error in validation relationships",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":
- "[user:jimmy] is <document:som>"`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "invalid resource and relation: `document:som`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "document:som",
				Line:    2,
				Column:  3,
			},
			``,
		},
		{
			"different relations",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":
- "[user:jimmy] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, found different relationships for subject `user:jimmy`: Specified: `<document:somedoc#viewer>`, Computed: `<document:somedoc#writer>`",
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: `[user:jimmy] is <document:somedoc#viewer>`,
				Line:    2,
				Column:  3,
			},
			`document:somedoc#view:
- '[user:jimmy] is <document:somedoc#writer>'
`,
		},
		{
			"full valid",
			`
			definition user {}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}

			definition document {
				relation writer: user
				relation viewer: user | user with testcaveat
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#writer@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:jake"),
				tuple.MustParse("document:somedoc#viewer@user:sarah[testcaveat]"),
				tuple.MustParse(`document:somedoc#viewer@user:tom[testcaveat:{"somecondition": 42}]`),
				tuple.MustParse(`document:somedoc#viewer@user:fred[testcaveat:{"somecondition": 53}]`),
			},
			`"document:somedoc#view":
- '[user:fred[...]] is <document:somedoc#viewer>'
- '[user:jake] is <document:somedoc#viewer>'
- '[user:jimmy] is <document:somedoc#writer>'
- '[user:sarah[...]] is <document:somedoc#viewer>'
- '[user:tom[...]] is <document:somedoc#viewer>'
`,
			`assertTrue:
- document:somedoc#writer@user:jimmy
- document:somedoc#view@user:jimmy
- document:somedoc#viewer@user:jake
- document:somedoc#viewer@user:tom
- 'document:somedoc#viewer@user:sarah with {"somecondition": "42"}'
assertCaveated:
- document:somedoc#viewer@user:sarah
assertFalse:
- document:somedoc#writer@user:sarah
- document:somedoc#writer@user:jake
- document:somedoc#viewer@user:fred
- 'document:somedoc#viewer@user:sarah with {"somecondition": "45"}'
`,
			nil,
			`document:somedoc#view:
- '[user:fred[...]] is <document:somedoc#viewer>'
- '[user:jake] is <document:somedoc#viewer>'
- '[user:jimmy] is <document:somedoc#writer>'
- '[user:sarah[...]] is <document:somedoc#viewer>'
- '[user:tom[...]] is <document:somedoc#viewer>'
`,
		},
		{
			"multipath",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#writer@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:jimmy"),
			},
			`"document:somedoc#view":
- "[user:jimmy] is <document:somedoc#writer>/<document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy
`,
			nil,
			`document:somedoc#view:
- '[user:jimmy] is <document:somedoc#viewer>/<document:somedoc#writer>'
`,
		},
		{
			"multipath missing relationship",
			`
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#writer@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:jimmy"),
			},
			`"document:somedoc#view":
- "[user:jimmy] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy
`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, found different relationships for subject `user:jimmy`: Specified: `<document:somedoc#writer>`, Computed: `<document:somedoc#viewer>/<document:somedoc#writer>`",
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: `[user:jimmy] is <document:somedoc#writer>`,
				Line:    2,
				Column:  3,
			},
			`document:somedoc#view:
- '[user:jimmy] is <document:somedoc#viewer>/<document:somedoc#writer>'
`,
		},
		{
			"invalid namespace on tuple",
			`
			definition user {}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			``,
			``,
			&devinterface.DeveloperError{
				Message: "object definition `document` not found",
				Kind:    devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
				Source:  devinterface.DeveloperError_RELATIONSHIP,
				Context: `document:somedoc#writer@user:jimmy`,
			},
			``,
		},
		{
			"invalid relation on tuple",
			`
			definition user {}
			definition document {}
			`,
			[]*core.RelationTuple{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			``,
			``,
			&devinterface.DeveloperError{
				Message: "relation/permission `writer` not found under definition `document`",
				Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
				Source:  devinterface.DeveloperError_RELATIONSHIP,
				Context: `document:somedoc#writer@user:jimmy`,
			},
			``,
		},
		{
			"wildcard relationship",
			`
		   			definition user {}
		   			definition document {
		   				relation writer: user
		   				relation viewer: user | user:*
		   				permission view = viewer + writer
		   			}
		   			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#writer@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:*"),
			},
			`"document:somedoc#view":
- "[user:*] is <document:somedoc#viewer>"
- "[user:jimmy] is <document:somedoc#viewer>/<document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy
- document:somedoc#viewer@user:jimmy
- document:somedoc#viewer@user:somegal
assertFalse:
- document:somedoc#writer@user:somegal`,
			nil,
			`document:somedoc#view:
- '[user:*] is <document:somedoc#viewer>'
- '[user:jimmy] is <document:somedoc#writer>'
`,
		},
		{
			"wildcard exclusion",
			`
		   			definition user {}
		   			definition document {
		   				relation banned: user
		   				relation viewer: user | user:*
		   				permission view = viewer - banned
		   			}
		   			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#banned@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:*"),
			},
			`"document:somedoc#view":
- "[user:* - {user:jimmy}] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#view@user:somegal
assertFalse:
- document:somedoc#view@user:jimmy`,
			nil,
			`document:somedoc#view:
- '[user:* - {user:jimmy}] is <document:somedoc#viewer>'
`,
		},
		{
			"wildcard exclusion under intersection",
			`
		   			definition user {}
		   			definition document {
		   				relation banned: user
		   				relation viewer: user | user:*
		   				relation other: user
		   				permission view = (viewer - banned) & (viewer - other)
		   			}
		   			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#other@user:sarah"),
				tuple.MustParse("document:somedoc#banned@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:*"),
			},
			`"document:somedoc#view":
- "[user:* - {user:jimmy}] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#view@user:somegal
assertFalse:
- document:somedoc#view@user:jimmy
- document:somedoc#view@user:sarah`,
			nil,
			`document:somedoc#view:
- '[user:* - {user:jimmy, user:sarah}] is <document:somedoc#viewer>'
`,
		},
		{
			"nil handling",
			`
		   			definition user {}
		   			definition document {
		   				relation viewer: user
		   				permission view = viewer
						permission empty = nil
		   			}
		   			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#viewer@user:jill"),
				tuple.MustParse("document:somedoc#viewer@user:tom"),
			},
			`"document:somedoc#view":
- "[user:jill] is <document:somedoc#viewer>"
- "[user:tom] is <document:somedoc#viewer>"
"document:somedoc#empty": []`,
			`assertTrue:
- document:somedoc#view@user:jill
- document:somedoc#view@user:tom
assertFalse:
- document:somedoc#empty@user:jill
- document:somedoc#empty@user:tom`,
			nil,
			"document:somedoc#empty: []\ndocument:somedoc#view:\n- '[user:jill] is <document:somedoc#viewer>'\n- '[user:tom] is <document:somedoc#viewer>'\n",
		},
		{
			"no expected subject or relation",
			`
		   			definition user {}
		   			definition document {
		   				relation viewer: user
		   				permission view = viewer
		   			}
		   			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#viewer@user:jill"),
				tuple.MustParse("document:somedoc#viewer@user:tom"),
			},
			`"document:somedoc#view":
- "is <document:somedoc#viewer>"
- "[user:tom] is "`,
			`assertTrue:
- document:somedoc#view@user:jill
- document:somedoc#view@user:tom`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, no expected subject specified in `is <document:somedoc#viewer>`",
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: `is <document:somedoc#viewer>`,
				Line:    2,
				Column:  3,
			},
			"document:somedoc#view:\n- '[user:jill] is <document:somedoc#viewer>'\n- '[user:tom] is <document:somedoc#viewer>'\n",
		},

		{
			"expected relations containing uncaveated subject that should be caveated",
			`
			definition user {}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}

			definition document {
				relation viewer: user with testcaveat
				permission view = viewer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#viewer@user:sarah[testcaveat]"),
			},
			`"document:somedoc#view":
- '[user:sarah] is <document:somedoc#viewer>'
`,
			`assertTrue:
- 'document:somedoc#viewer@user:sarah with {"somecondition": "42"}'
assertCaveated:
- document:somedoc#viewer@user:sarah
assertFalse:
- 'document:somedoc#viewer@user:sarah with {"somecondition": "45"}'
`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, found caveat mismatch",
				Line:    2,
				Column:  3,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Kind:    devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Context: "[user:sarah] is <document:somedoc#viewer>",
			},
			`document:somedoc#view:
- '[user:sarah[...]] is <document:somedoc#viewer>'
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			response := run(t, &devinterface.DeveloperRequest{
				Context: &devinterface.RequestContext{
					Schema:        tc.schema,
					Relationships: tc.relationships,
				},
				Operations: []*devinterface.Operation{
					{
						AssertionsParameters: &devinterface.RunAssertionsParameters{
							AssertionsYaml: tc.assertionsYaml,
						},
					},
					{
						ValidationParameters: &devinterface.RunValidationParameters{
							ValidationYaml: tc.validationYaml,
						},
					},
				},
			})

			if tc.expectedError != nil {
				errors := []*devinterface.DeveloperError{}

				if response.GetDeveloperErrors() != nil {
					errors = append(errors, response.GetDeveloperErrors().InputErrors...)
				} else {
					require.NotNil(response.GetOperationsResults(), "found nil results: %v", response)
					require.GreaterOrEqual(len(response.GetOperationsResults().Results), 2, "found insufficient results")

					if response.GetOperationsResults().Results[0].GetAssertionsResult().InputError != nil {
						errors = append(errors, response.GetOperationsResults().Results[0].GetAssertionsResult().InputError)
					}

					if response.GetOperationsResults().Results[1].GetValidationResult().InputError != nil {
						errors = append(errors, response.GetOperationsResults().Results[1].GetValidationResult().InputError)
					}

					if len(response.GetOperationsResults().Results[0].GetAssertionsResult().ValidationErrors) > 0 {
						errors = append(errors, response.GetOperationsResults().Results[0].GetAssertionsResult().ValidationErrors...)
					}

					if len(response.GetOperationsResults().Results[1].GetValidationResult().ValidationErrors) > 0 {
						errors = append(errors, response.GetOperationsResults().Results[1].GetValidationResult().ValidationErrors...)
					}
				}
				testutil.RequireProtoEqual(t, tc.expectedError, errors[0], "mismatch on errors")
			} else {
				require.Equal(0, len(response.GetOperationsResults().Results[0].GetAssertionsResult().ValidationErrors), "Failed assertion", response.GetOperationsResults().Results[0].GetAssertionsResult().ValidationErrors)
			}

			if tc.expectedValidationYaml != "" && response.GetOperationsResults() != nil {
				require.Equal(tc.expectedValidationYaml, response.GetOperationsResults().Results[1].GetValidationResult().UpdatedValidationYaml)
			}
		})
	}
}
