//go:build wasm
// +build wasm

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

type editCheckResult struct {
	Relationship  tuple.Relationship
	IsMember      bool
	Error         *devinterface.DeveloperError
	IsConditional bool
}

func TestSchemaWarningsOperation(t *testing.T) {
	type testCase struct {
		name            string
		schema          string
		expectedWarning *devinterface.DeveloperWarning
	}

	tests := []testCase{
		{
			"no warnings",
			`definition foo {
				relation bar: foo
			}`,
			nil,
		},
		{
			"permission misnamed",
			`definition resource {
				permission view_resource = nil
			}`,
			&devinterface.DeveloperWarning{
				Message:    "Permission \"view_resource\" references parent type \"resource\" in its name; it is recommended to drop the suffix (relation-name-references-parent)",
				Line:       2,
				Column:     5,
				SourceCode: "view_resource",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response := run(t, &devinterface.DeveloperRequest{
				Context: &devinterface.RequestContext{
					Schema: tc.schema,
				},
				Operations: []*devinterface.Operation{
					{
						SchemaWarningsParameters: &devinterface.SchemaWarningsParameters{},
					},
				},
			})

			require.Empty(t, response.GetDeveloperErrors())

			if tc.expectedWarning == nil {
				require.Empty(t, response.GetOperationsResults().Results[0].SchemaWarningsResult.Warnings)
			} else {
				testutil.RequireProtoEqual(t, tc.expectedWarning, response.OperationsResults.Results[0].SchemaWarningsResult.Warnings[0], "mismatching warning")
			}
		})
	}
}

func TestCheckOperation(t *testing.T) {
	type testCase struct {
		name              string
		schema            string
		relationships     []tuple.Relationship
		checkRelationship tuple.Relationship
		caveatContext     map[string]any
		expectedError     *devinterface.DeveloperError
		expectedResult    *editCheckResult
	}

	tests := []testCase{
		{
			"invalid keyword",
			`def foo {
				relation bar:
			}`,
			[]tuple.Relationship{},
			tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "Unexpected token at root level: TokenTypeIdentifier",
				Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
				Source:  devinterface.DeveloperError_SCHEMA,
				Line:    1,
				Column:  1,
				Context: "def",
			},
			nil,
		},
		{
			"invalid namespace",
			`definition foo {
				relation bar:
			}`,
			[]tuple.Relationship{},
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
			[]tuple.Relationship{},
			tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "error in object definition fo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
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
			[]tuple.Relationship{},
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{},
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
			[]tuple.Relationship{
				tuple.MustParse("document:someobj#viewer@document:someobj#viewer"),
			},
			tuple.MustParse("document:someobj#viewer@user:foo"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("document:someobj#viewer@user:foo"),
				Error: &devinterface.DeveloperError{
					Message: "max depth exceeded: this usually indicates a recursive or too deep data dependency. See: https://spicedb.dev/d/debug-max-depth",
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
		{
			"invalid relationship subject type",
			`definition user {}
			
			definition resource {
				relation viewer: user
				 permission view = viewer
			}`,
			[]tuple.Relationship{tuple.MustParse("resource:someobj#viewer@resource:foo")},
			tuple.MustParse("resource:someobj#view@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "subjects of type `resource` are not allowed on relation `resource#viewer`",
				Kind:    devinterface.DeveloperError_INVALID_SUBJECT_TYPE,
				Source:  devinterface.DeveloperError_RELATIONSHIP,
				Context: "resource:someobj#viewer@resource:foo",
			},
			nil,
		},
		{
			"invalid relationship with caveated subject type",
			`definition user {}
			
			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation viewer: user with somecaveat
				 permission view = viewer
			}`,
			[]tuple.Relationship{tuple.MustParse("resource:someobj#viewer@user:foo")},
			tuple.MustParse("resource:someobj#view@user:foo"),
			nil,
			&devinterface.DeveloperError{
				Message: "subjects of type `user` are not allowed on relation `resource#viewer` without one of the following caveats: somecaveat",
				Kind:    devinterface.DeveloperError_INVALID_SUBJECT_TYPE,
				Source:  devinterface.DeveloperError_RELATIONSHIP,
				Context: "resource:someobj#viewer@user:foo",
			},
			nil,
		},
		{
			"valid extended ID",
			`
				definition user {}
				definition somenamespace {
					permission empty = nil
				}
			`,
			[]tuple.Relationship{},
			tuple.MustParse("somenamespace:--=base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#empty@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong"),
			nil,
			nil,
			&editCheckResult{
				Relationship: tuple.MustParse("somenamespace:--=base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#empty@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong"),
				IsMember:     false,
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

			relationships := make([]*corev1.RelationTuple, 0, len(tc.relationships))
			for _, rel := range tc.relationships {
				relationships = append(relationships, rel.ToCoreTuple())
			}

			response := run(t, &devinterface.DeveloperRequest{
				Context: &devinterface.RequestContext{
					Schema:        tc.schema,
					Relationships: relationships,
				},
				Operations: []*devinterface.Operation{
					{
						CheckParameters: &devinterface.CheckOperationParameters{
							Resource:      tc.checkRelationship.Resource.ToCoreONR(),
							Subject:       tc.checkRelationship.Subject.ToCoreONR(),
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
		relationships          []tuple.Relationship
		validationYaml         string
		assertionsYaml         string
		expectedError          *devinterface.DeveloperError
		expectCheckTraces      bool
		expectedValidationYaml string
	}

	tests := []testCase{
		{
			"valid namespace",
			`definition somenamespace {}`,
			[]tuple.Relationship{},
			"",
			"",
			nil,
			false,
			"{}\n",
		},
		{
			"invalid validation yaml",
			`definition somenamespace {}`,
			[]tuple.Relationship{},
			`asdkjhgasd`,
			"",
			&devinterface.DeveloperError{
				Message: "unexpected value `asdkjhg`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "asdkjhg",
				Line:    1,
			},
			false,
			"",
		},
		{
			"invalid assertions yaml",
			`definition somenamespace {}`,
			[]tuple.Relationship{},
			"",
			`asdhasjdkhjasd`,
			&devinterface.DeveloperError{
				Message: "unexpected value `asdhasj`",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_ASSERTION,
				Context: "asdhasj",
				Line:    1,
			},
			false,
			"",
		},
		{
			"assertions yaml with garbage",
			`definition somenamespace {}`,
			[]tuple.Relationship{},
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
			false,
			"",
		},
		{
			"assertions yaml with indented garbage",
			`definition somenamespace {}`,
			[]tuple.Relationship{},
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
			false,
			"",
		},
		{
			"invalid assertions true yaml",
			`definition somenamespace {}`,
			[]tuple.Relationship{},
			"",
			`assertTrue:
- something`,
			&devinterface.DeveloperError{
				Message: "error parsing relationship in assertion `something`: invalid relationship string",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_ASSERTION,
				Line:    2,
				Column:  3,
				Context: "something",
			},
			false,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#viewer@user:jimmy")},
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
			true,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#viewer@user:jimmy")},
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
			true,
			"{}\n",
		},
		{
			"assertion invalid caveated relation",
			`
				definition user {}
				definition document {}
			`,
			[]tuple.Relationship{},
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
			false,
			"{}\n",
		},
		{
			"assertion invalid relation",
			`
				definition user {}
				definition document {}
			`,
			[]tuple.Relationship{},
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
			false,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, subject `user:jimmy` found but not listed in expected subjects",
				Kind:    devinterface.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "document:somedoc#view",
				Line:    1,
				Column:  1,
			},
			false,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
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
			false,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":
- "[user] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "invalid subject: `user`: invalid subject ONR: user",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "user",
				Line:    2,
				Column:  3,
			},
			false,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			`"document:somedoc#view":
- "[user:jimmy] is <document:som>"`,
			`assertTrue:
- document:somedoc#view@user:jimmy`,
			&devinterface.DeveloperError{
				Message: "invalid resource and relation: `document:som`: invalid ONR: document:som",
				Kind:    devinterface.DeveloperError_PARSE_ERROR,
				Source:  devinterface.DeveloperError_VALIDATION_YAML,
				Context: "document:som",
				Line:    2,
				Column:  3,
			},
			false,
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
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
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
			false,
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
			[]tuple.Relationship{
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
			false,
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
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#writer@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:jimmy"),
			},
			`"document:somedoc#view":
- "[user:jimmy] is <document:somedoc#writer>/<document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy
`,
			nil,
			false,
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
			[]tuple.Relationship{
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
			false,
			`document:somedoc#view:
- '[user:jimmy] is <document:somedoc#viewer>/<document:somedoc#writer>'
`,
		},
		{
			"invalid namespace on tuple",
			`
			definition user {}
			`,
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			``,
			``,
			&devinterface.DeveloperError{
				Message: "object definition `document` not found",
				Kind:    devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
				Source:  devinterface.DeveloperError_RELATIONSHIP,
				Context: `document:somedoc#writer@user:jimmy`,
			},
			false,
			``,
		},
		{
			"invalid relation on tuple",
			`
			definition user {}
			definition document {}
			`,
			[]tuple.Relationship{tuple.MustParse("document:somedoc#writer@user:jimmy")},
			``,
			``,
			&devinterface.DeveloperError{
				Message: "relation/permission `writer` not found under definition `document`",
				Kind:    devinterface.DeveloperError_UNKNOWN_RELATION,
				Source:  devinterface.DeveloperError_RELATIONSHIP,
				Context: `document:somedoc#writer@user:jimmy`,
			},
			false,
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
			[]tuple.Relationship{
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
			false,
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
			[]tuple.Relationship{
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
			false,
			`document:somedoc#view:
- '[user:* - {user:jimmy}] is <document:somedoc#viewer>'
`,
		},
		{
			"wildcard multiple exclusion",
			`
		   			definition user {}
		   			definition document {
		   				relation banned: user
		   				relation viewer: user | user:*
		   				permission view = viewer - banned
		   			}
		   			`,
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#banned@user:jimmy"),
				tuple.MustParse("document:somedoc#banned@user:fred"),
				tuple.MustParse("document:somedoc#viewer@user:*"),
			},
			`"document:somedoc#view":
- "[user:* - {user:fred, user:jimmy}] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#view@user:somegal
assertFalse:
- document:somedoc#view@user:jimmy
- document:somedoc#view@user:fred`,
			nil,
			false,
			`document:somedoc#view:
- '[user:* - {user:fred, user:jimmy}] is <document:somedoc#viewer>'
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
			[]tuple.Relationship{
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
			false,
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
			[]tuple.Relationship{
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
			false,
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
			[]tuple.Relationship{
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
			false,
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
			[]tuple.Relationship{
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
			false,
			`document:somedoc#view:
- '[user:sarah[...]] is <document:somedoc#viewer>'
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			relationships := make([]*corev1.RelationTuple, 0, len(tc.relationships))
			for _, rel := range tc.relationships {
				relationships = append(relationships, rel.ToCoreTuple())
			}

			response := run(t, &devinterface.DeveloperRequest{
				Context: &devinterface.RequestContext{
					Schema:        tc.schema,
					Relationships: relationships,
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

				if tc.expectCheckTraces {
					require.NotNil(t, errors[0].CheckDebugInformation)
					require.NotNil(t, errors[0].CheckResolvedDebugInformation)

					// Unset these values to avoid the need to specify above
					// in the test data. This is necessary because the debug
					// information contains the revision timestamp, which changes
					// on every call.
					errors[0].CheckDebugInformation = nil
					errors[0].CheckResolvedDebugInformation = nil
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
