package v0

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestDeveloperSharing(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	// Check for non-existent share.
	resp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: "someref",
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_UNKNOWN_REFERENCE, resp.Status)

	// Add a share resource.
	sresp, err := srv.Share(context.Background(), &v0.ShareRequest{
		Schema:            "s",
		RelationshipsYaml: "ry",
		ValidationYaml:    "vy",
		AssertionsYaml:    "ay",
	})
	require.NoError(err)

	// Lookup again.
	lresp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: sresp.ShareReference,
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_VALID_REFERENCE, lresp.Status)
	require.Equal("s", lresp.Schema)
}

func TestDeveloperSharingConverted(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	// Add a share resource in V1 format.
	store.(*inMemoryShareStore).shared["foo"] = []byte(`{
		"version": "1",
		"namespace_configs": [
			"name: \"foo\""
		],
		"relation_tuples": "rt",
		"validation_yaml": "vy",
		"assertions_yaml": "ay"
}`)

	// Lookup and ensure converted.
	lresp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: "foo",
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_UPGRADED_REFERENCE, lresp.Status)
	require.Equal("rt", lresp.RelationshipsYaml)
	require.Equal("vy", lresp.ValidationYaml)
	require.Equal("ay", lresp.AssertionsYaml)

	require.Equal("definition foo {}\n\n", lresp.Schema)
}

func TestEditCheck(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	type testCase struct {
		name               string
		schema             string
		relationships      []*core.RelationTuple
		checkRelationships []*core.RelationTuple
		expectedError      *v0.DeveloperError
		expectedResults    []*v0.EditCheckResult
	}

	tests := []testCase{
		{
			"invalid namespace",
			`definition foo {
				relation bar:
			}`,
			[]*core.RelationTuple{},
			[]*core.RelationTuple{},
			&v0.DeveloperError{
				Message: "Expected identifier, found token TokenTypeRightBrace",
				Kind:    v0.DeveloperError_SCHEMA_ISSUE,
				Source:  v0.DeveloperError_SCHEMA,
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
			[]*core.RelationTuple{},
			&v0.DeveloperError{
				Message: "error in object definition fo: invalid NamespaceDefinition.Name: value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)?[a-z][a-z0-9_]{1,62}[a-z0-9]$\"",
				Kind:    v0.DeveloperError_SCHEMA_ISSUE,
				Source:  v0.DeveloperError_SCHEMA,
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
			[]*core.RelationTuple{},
			&v0.DeveloperError{
				Message: "found duplicate relation/permission name `writer`",
				Kind:    v0.DeveloperError_SCHEMA_ISSUE,
				Source:  v0.DeveloperError_SCHEMA,
				Line:    5,
				Column:  6,
				Context: "writer",
			},
			nil,
		},
		{
			"valid namespace",
			`definition foos {}`,
			[]*core.RelationTuple{},
			[]*core.RelationTuple{},
			nil,
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
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#anotherrel@user:foo"),
			},
			nil,
			[]*v0.EditCheckResult{
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#anotherrel@user:foo")),
					Error: &v0.DeveloperError{
						Message: "relation/permission `anotherrel` not found under definition `somenamespace`",
						Kind:    v0.DeveloperError_UNKNOWN_RELATION,
						Source:  v0.DeveloperError_CHECK_WATCH,
						Context: "somenamespace:someobj#anotherrel@user:foo",
					},
				},
			},
		},
		{
			"valid checks",
			`
				definition user {}
				definition somenamespace {
					relation somerel: user
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
			},
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				tuple.MustParse("somenamespace:someobj#somerel@user:anotheruser"),
			},
			nil,
			[]*v0.EditCheckResult{
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#somerel@user:foo")),
					IsMember:     true,
				},
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#somerel@user:anotheruser")),
					IsMember:     false,
				},
			},
		},
		{
			"valid wildcard checks",
			`
				definition user {}
				definition somenamespace {
					relation somerel: user | user:*
				}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:*"),
			},
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#somerel@user:foo"),
				tuple.MustParse("somenamespace:someobj#somerel@user:anotheruser"),
			},
			nil,
			[]*v0.EditCheckResult{
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#somerel@user:foo")),
					IsMember:     true,
				},
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#somerel@user:anotheruser")),
					IsMember:     true,
				},
			},
		},
		{
			"valid nil checks",
			`
				definition user {}
				definition somenamespace {
					permission empty = nil
				}
			`,
			[]*core.RelationTuple{},
			[]*core.RelationTuple{
				tuple.MustParse("somenamespace:someobj#empty@user:foo"),
				tuple.MustParse("somenamespace:someobj#empty@user:anotheruser"),
			},
			nil,
			[]*v0.EditCheckResult{
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#empty@user:foo")),
					IsMember:     false,
				},
				{
					Relationship: core.ToV0RelationTuple(tuple.MustParse("somenamespace:someobj#empty@user:anotheruser")),
					IsMember:     false,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewInMemoryShareStore("flavored")
			srv := NewDeveloperServer(store)

			resp, err := srv.EditCheck(context.Background(), &v0.EditCheckRequest{
				Context: &v0.RequestContext{
					Schema:        tc.schema,
					Relationships: core.ToV0RelationTuples(tc.relationships),
				},
				CheckRelationships: core.ToV0RelationTuples(tc.checkRelationships),
			})
			require.NoError(t, err)

			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError, resp.RequestErrors[0])
			} else {
				require.Len(t, resp.RequestErrors, 0, "found error(s): %v", resp.RequestErrors)
			}
			testutil.RequireEqualEmptyNil(t, tc.expectedResults, resp.CheckResults)
		})
	}
}

func TestDeveloperValidate(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	type testCase struct {
		name                   string
		schema                 string
		relationships          []*core.RelationTuple
		validationYaml         string
		assertionsYaml         string
		expectedError          *v0.DeveloperError
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
			&v0.DeveloperError{
				Message: "unexpected value `asdkjhg`",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_VALIDATION_YAML,
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
			&v0.DeveloperError{
				Message: "unexpected value `asdhasj`",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_ASSERTION,
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
			&v0.DeveloperError{
				Message: "did not find expected key",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_ASSERTION,
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
			&v0.DeveloperError{
				Message: "unexpected value `garbage`",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_ASSERTION,
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
			&v0.DeveloperError{
				Message: "error parsing relationship `something`",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_ASSERTION,
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
			&v0.DeveloperError{
				Message: "Expected relation or permission document:somedoc#viewer@user:jake to exist",
				Kind:    v0.DeveloperError_ASSERTION_FAILED,
				Source:  v0.DeveloperError_ASSERTION,
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
			&v0.DeveloperError{
				Message: "Expected relation or permission document:somedoc#viewer@user:jimmy to not exist",
				Kind:    v0.DeveloperError_ASSERTION_FAILED,
				Source:  v0.DeveloperError_ASSERTION,
				Context: "document:somedoc#viewer@user:jimmy",
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
			&v0.DeveloperError{
				Message: "relation/permission `viewer` not found under definition `document`",
				Kind:    v0.DeveloperError_UNKNOWN_RELATION,
				Source:  v0.DeveloperError_ASSERTION,
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
			&v0.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, subject `user:jimmy` found but missing from specified",
				Kind:    v0.DeveloperError_EXTRA_RELATIONSHIP_FOUND,
				Source:  v0.DeveloperError_VALIDATION_YAML,
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
			&v0.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, missing expected subject `user:jake`",
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  v0.DeveloperError_VALIDATION_YAML,
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
			&v0.DeveloperError{
				Message: "invalid subject: `user`",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Context: "[user]",
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
			&v0.DeveloperError{
				Message: "invalid resource and relation: `document:som`",
				Kind:    v0.DeveloperError_PARSE_ERROR,
				Source:  v0.DeveloperError_VALIDATION_YAML,
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
			&v0.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, found different relationships for subject `user:jimmy`: Specified: `<document:somedoc#viewer>`, Computed: `<document:somedoc#writer>`",
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  v0.DeveloperError_VALIDATION_YAML,
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
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#writer@user:jimmy"),
				tuple.MustParse("document:somedoc#viewer@user:jake"),
			},
			`"document:somedoc#view":
- "[user:jimmy] is <document:somedoc#writer>"
- "[user:jake] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy
- document:somedoc#viewer@user:jimmy
- document:somedoc#viewer@user:jake
assertFalse:
- document:somedoc#writer@user:jake
`,
			nil,
			`document:somedoc#view:
- '[user:jake] is <document:somedoc#viewer>'
- '[user:jimmy] is <document:somedoc#writer>'
`,
		},
		{
			"muiltipath",
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
			"muiltipath missing relationship",
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
			&v0.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, found different relationships for subject `user:jimmy`: Specified: `<document:somedoc#writer>`, Computed: `<document:somedoc#viewer>/<document:somedoc#writer>`",
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  v0.DeveloperError_VALIDATION_YAML,
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
			&v0.DeveloperError{
				Message: "object definition `document` not found",
				Kind:    v0.DeveloperError_UNKNOWN_OBJECT_TYPE,
				Source:  v0.DeveloperError_RELATIONSHIP,
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
			&v0.DeveloperError{
				Message: "relation/permission `writer` not found under definition `document`",
				Kind:    v0.DeveloperError_UNKNOWN_RELATION,
				Source:  v0.DeveloperError_RELATIONSHIP,
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
			&v0.DeveloperError{
				Message: "For object and permission/relation `document:somedoc#view`, no expected subject specified in `is <document:somedoc#viewer>`",
				Kind:    v0.DeveloperError_MISSING_EXPECTED_RELATIONSHIP,
				Source:  v0.DeveloperError_VALIDATION_YAML,
				Context: `is <document:somedoc#viewer>`,
				Line:    2,
				Column:  3,
			},
			"document:somedoc#view:\n- '[user:jill] is <document:somedoc#viewer>'\n- '[user:tom] is <document:somedoc#viewer>'\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			store := NewInMemoryShareStore("flavored")
			srv := NewDeveloperServer(store)

			resp, err := srv.Validate(context.Background(), &v0.ValidateRequest{
				Context: &v0.RequestContext{
					Schema:        tc.schema,
					Relationships: core.ToV0RelationTuples(tc.relationships),
				},
				ValidationYaml:       tc.validationYaml,
				AssertionsYaml:       tc.assertionsYaml,
				UpdateValidationYaml: true,
			})
			require.NoError(err)

			if tc.expectedError != nil {
				if len(resp.RequestErrors) > 0 {
					require.Equal(tc.expectedError, resp.RequestErrors[0])
				} else {
					require.True(len(resp.ValidationErrors) > 0)
					require.Equal(tc.expectedError, resp.ValidationErrors[0])
				}
			} else {
				require.Equal(0, len(resp.RequestErrors), "Found error(s): %v", resp.RequestErrors)
			}

			require.Equal(tc.expectedValidationYaml, resp.UpdatedValidationYaml)
		})
	}
}

func TestDeveloperFormatSchema(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	lresp, err := srv.FormatSchema(context.Background(), &v0.FormatSchemaRequest{
		Schema: "definition foos {} definition bars{}",
	})

	require.NoError(err)
	require.Equal("definition foos {}\n\ndefinition bars {}", lresp.FormattedSchema)
}

func TestDeveloperValidateONR(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	resp, err := srv.Validate(context.Background(), &v0.ValidateRequest{
		Context: &v0.RequestContext{
			Schema: `
			definition user {}
			definition document {
				relation writer: user
				relation viewer: user
				permission view = viewer + writer
			}
			`,
			Relationships: []*v0.RelationTuple{
				{
					ObjectAndRelation: &v0.ObjectAndRelation{
						Namespace: "document",
						ObjectId:  "somedoc",
						Relation:  "writerIsNotValid",
					},
					User: &v0.User{
						UserOneof: &v0.User_Userset{
							Userset: &v0.ObjectAndRelation{
								Namespace: "user",
								ObjectId:  "jimmy",
								Relation:  "...",
							},
						},
					},
				},
			},
		},
		ValidationYaml:       "",
		AssertionsYaml:       "",
		UpdateValidationYaml: false,
	})
	require.NoError(err)
	require.Equal(1, len(resp.RequestErrors))
	require.Equal(&v0.DeveloperError{
		Message: "invalid RelationTuple.ObjectAndRelation: embedded message failed validation | caused by: invalid ObjectAndRelation.Relation: value does not match regex pattern \"^(\\\\.\\\\.\\\\.|[a-z][a-z0-9_]{1,62}[a-z0-9])$\"",
		Kind:    v0.DeveloperError_PARSE_ERROR,
		Source:  v0.DeveloperError_RELATIONSHIP,
		Context: `document:somedoc#writerIsNotValid@user:jimmy`,
	}, resp.RequestErrors[0])
}
