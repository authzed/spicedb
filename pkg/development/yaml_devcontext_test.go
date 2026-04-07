package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestNewDevContextForYAML(t *testing.T) {
	tests := []struct {
		name            string
		yaml            string
		expectDevErr    bool
		expectInternErr bool
		expectedSource  devinterface.DeveloperError_Source
		expectedLine    uint32
		expectedColumn  uint32
		expectedKind    devinterface.DeveloperError_ErrorKind
		expectedMsgSub  string
	}{
		{
			name: "valid file",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@user:alice
assertions:
  assertTrue:
    - document:doc1#viewer@user:alice
`,
		},
		{
			name: "schema parse error on line 3 of yaml",
			yaml: `schema: |
  definition user {}
  invalid syntax here
relationships: ""
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_SCHEMA,
			expectedKind:   devinterface.DeveloperError_SCHEMA_ISSUE,
			expectedLine:   3,
		},
		{
			name: "schema parse error later in schema block",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
  this is invalid
relationships: ""
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_SCHEMA,
			expectedKind:   devinterface.DeveloperError_SCHEMA_ISSUE,
			expectedLine:   6,
		},
		{
			name: "schema validation error - unknown relation reference",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
    permission view = nonexistent
  }
relationships: ""
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_SCHEMA,
			expectedKind:   devinterface.DeveloperError_SCHEMA_ISSUE,
			expectedLine:   5,
		},
		{
			name: "relationship validation error - unknown object type",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@nonexistent:foo
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_RELATIONSHIP,
			expectedKind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
			expectedLine:   7,
		},
		{
			name: "relationships before schema - schema error",
			yaml: `relationships: ""
schema: |
  definition user {}
  invalid syntax here
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_SCHEMA,
			expectedKind:   devinterface.DeveloperError_SCHEMA_ISSUE,
			expectedLine:   4,
		},
		{
			name: "relationships before schema - rel error",
			yaml: `relationships: |
  document:doc1#viewer@nonexistent:foo
schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_RELATIONSHIP,
			expectedKind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
			expectedLine:   2,
		},
		{
			name: "error on second relationship",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@user:alice
  document:doc1#viewer@nonexistent:foo
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_RELATIONSHIP,
			expectedKind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
			expectedLine:   8,
		},
		{
			name: "error on third relationship",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@user:alice
  document:doc1#viewer@user:bob
  document:doc1#viewer@nonexistent:foo
`,
			expectDevErr:   true,
			expectedSource: devinterface.DeveloperError_RELATIONSHIP,
			expectedKind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
			expectedLine:   9,
		},
		{
			name: "valid file with assertions and validation returned",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
    permission view = viewer
  }
relationships: |
  document:doc1#viewer@user:alice
assertions:
  assertTrue:
    - document:doc1#view@user:alice
  assertFalse:
    - document:doc1#view@user:bob
`,
		},
		{
			name:            "malformed yaml",
			yaml:            `{invalid yaml: [`,
			expectInternErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, devErrs, err := NewDevContextForYAML(t.Context(), []byte(tt.yaml))

			if tt.expectInternErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if !tt.expectDevErr {
				require.Nil(t, devErrs, "expected no developer errors")
				require.NotNil(t, result)
				require.NotNil(t, result.DevContext)
				result.DevContext.Dispose()
				return
			}

			require.NotNil(t, devErrs, "expected developer errors")
			require.NotEmpty(t, devErrs.InputErrors)

			devErr := devErrs.InputErrors[0]
			require.Equal(t, tt.expectedSource, devErr.Source, "error source mismatch")
			require.Equal(t, tt.expectedKind, devErr.Kind, "error kind mismatch")

			if tt.expectedLine > 0 {
				require.Equal(t, tt.expectedLine, devErr.Line, "error line mismatch (got error: %s)", devErr.Message)
			}
			if tt.expectedColumn > 0 {
				require.Equal(t, tt.expectedColumn, devErr.Column, "error column mismatch (got error: %s)", devErr.Message)
			}
			if tt.expectedMsgSub != "" {
				require.Contains(t, devErr.Message, tt.expectedMsgSub)
			}
		})
	}
}

func TestNewDevContextForYAMLMultipleErrors(t *testing.T) {
	type expectedError struct {
		source devinterface.DeveloperError_Source
		kind   devinterface.DeveloperError_ErrorKind
		line   uint32
	}

	tests := []struct {
		name     string
		yaml     string
		expected []expectedError
	}{
		{
			name: "multiple relationship validation errors",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@nonexistent:alice
  document:doc1#viewer@alsonotreal:bob
`,
			expected: []expectedError{
				{
					source: devinterface.DeveloperError_RELATIONSHIP,
					kind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
					line:   7,
				},
				{
					source: devinterface.DeveloperError_RELATIONSHIP,
					kind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
					line:   8,
				},
			},
		},
		{
			name: "multiple relationship errors with valid rels interspersed",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@user:alice
  document:doc1#viewer@nonexistent:bob
  document:doc1#viewer@user:charlie
  document:doc1#viewer@alsonotreal:dave
`,
			expected: []expectedError{
				{
					source: devinterface.DeveloperError_RELATIONSHIP,
					kind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
					line:   8,
				},
				{
					source: devinterface.DeveloperError_RELATIONSHIP,
					kind:   devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE,
					line:   10,
				},
			},
		},
		{
			name: "multiple schema validation errors across definitions",
			yaml: `schema: |
  definition user {}
  definition document {
    relation viewer: user
    permission view = nonexistent
  }
  definition folder {
    relation viewer: user
    permission view = alsononexistent
  }
relationships: ""
`,
			expected: []expectedError{
				{
					source: devinterface.DeveloperError_SCHEMA,
					kind:   devinterface.DeveloperError_SCHEMA_ISSUE,
					line:   5,
				},
				{
					source: devinterface.DeveloperError_SCHEMA,
					kind:   devinterface.DeveloperError_SCHEMA_ISSUE,
					line:   9,
				},
			},
		},
		{
			name: "multiple schema errors with relationships before schema",
			yaml: `relationships: ""
schema: |
  definition user {}
  definition document {
    relation viewer: user
    permission view = nonexistent
  }
  definition folder {
    relation viewer: user
    permission view = alsononexistent
  }
`,
			expected: []expectedError{
				{
					source: devinterface.DeveloperError_SCHEMA,
					kind:   devinterface.DeveloperError_SCHEMA_ISSUE,
					line:   6,
				},
				{
					source: devinterface.DeveloperError_SCHEMA,
					kind:   devinterface.DeveloperError_SCHEMA_ISSUE,
					line:   10,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, devErrs, err := NewDevContextForYAML(t.Context(), []byte(tt.yaml))
			require.NoError(t, err)
			require.NotNil(t, devErrs)
			require.Len(t, devErrs.InputErrors, len(tt.expected), "expected %d errors, got %d", len(tt.expected), len(devErrs.InputErrors))

			for i, exp := range tt.expected {
				devErr := devErrs.InputErrors[i]
				require.Equal(t, exp.source, devErr.Source, "error %d: source mismatch", i)
				require.Equal(t, exp.kind, devErr.Kind, "error %d: kind mismatch", i)
				if exp.line > 0 {
					require.Equal(t, exp.line, devErr.Line, "error %d: line mismatch (got error: %s)", i, devErr.Message)
				}
			}
		})
	}
}

func TestNewDevContextForYAMLAssertionsReturned(t *testing.T) {
	yaml := `schema: |
  definition user {}
  definition document {
    relation viewer: user
    permission view = viewer
  }
relationships: |
  document:doc1#viewer@user:alice
assertions:
  assertTrue:
    - document:doc1#view@user:alice
  assertFalse:
    - document:doc1#view@user:bob
`

	result, devErrs, err := NewDevContextForYAML(t.Context(), []byte(yaml))
	require.NoError(t, err)
	require.Nil(t, devErrs)
	require.NotNil(t, result)
	defer result.DevContext.Dispose()

	require.Len(t, result.Assertions.AssertTrue, 1)
	require.Len(t, result.Assertions.AssertFalse, 1)
	require.Equal(t, "document:doc1#view@user:alice", result.Assertions.AssertTrue[0].RelationshipWithContextString)
	require.Equal(t, "document:doc1#view@user:bob", result.Assertions.AssertFalse[0].RelationshipWithContextString)
}

func TestYAMLDevContextRunCheck(t *testing.T) {
	yamlContent := `schema: |
  definition user {}
  definition document {
    relation viewer: user
    relation editor: user
    permission view = viewer + editor
    permission edit = editor
  }
relationships: |
  document:doc1#viewer@user:alice
  document:doc1#editor@user:bob
  document:doc2#viewer@user:charlie
`

	result, devErrs, err := NewDevContextForYAML(t.Context(), []byte(yamlContent))
	require.NoError(t, err)
	require.Nil(t, devErrs)
	require.NotNil(t, result)
	defer result.DevContext.Dispose()

	tests := []struct {
		name           string
		resource       string
		subject        string
		expectedMember v1.ResourceCheckResult_Membership
	}{
		{
			name:           "viewer can view",
			resource:       "document:doc1#view",
			subject:        "user:alice",
			expectedMember: v1.ResourceCheckResult_MEMBER,
		},
		{
			name:           "viewer cannot edit",
			resource:       "document:doc1#edit",
			subject:        "user:alice",
			expectedMember: v1.ResourceCheckResult_NOT_MEMBER,
		},
		{
			name:           "editor can view via union",
			resource:       "document:doc1#view",
			subject:        "user:bob",
			expectedMember: v1.ResourceCheckResult_MEMBER,
		},
		{
			name:           "editor can edit",
			resource:       "document:doc1#edit",
			subject:        "user:bob",
			expectedMember: v1.ResourceCheckResult_MEMBER,
		},
		{
			name:           "unrelated user cannot view",
			resource:       "document:doc1#view",
			subject:        "user:charlie",
			expectedMember: v1.ResourceCheckResult_NOT_MEMBER,
		},
		{
			name:           "charlie can view doc2",
			resource:       "document:doc2#view",
			subject:        "user:charlie",
			expectedMember: v1.ResourceCheckResult_MEMBER,
		},
		{
			name:           "alice cannot view doc2",
			resource:       "document:doc2#view",
			subject:        "user:alice",
			expectedMember: v1.ResourceCheckResult_NOT_MEMBER,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkResult, err := RunCheck(
				result.DevContext,
				tuple.MustParseONR(tt.resource),
				tuple.MustParseSubjectONR(tt.subject),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, tt.expectedMember, checkResult.Permissionship)
		})
	}
}

func TestYAMLDevContextRunCheckWithCaveats(t *testing.T) {
	yamlContent := `schema: |
  definition user {}
  caveat is_weekday(day_of_week string) {
    day_of_week != "saturday" && day_of_week != "sunday"
  }
  definition document {
    relation viewer: user with is_weekday
    permission view = viewer
  }
relationships: |
  document:doc1#viewer@user:alice[is_weekday]
`

	result, devErrs, err := NewDevContextForYAML(t.Context(), []byte(yamlContent))
	require.NoError(t, err)
	require.Nil(t, devErrs)
	require.NotNil(t, result)
	defer result.DevContext.Dispose()

	// With caveat context satisfied: member
	checkResult, err := RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:alice"),
		map[string]any{"day_of_week": "monday"},
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.Permissionship)

	// With caveat context not satisfied: not member
	checkResult, err = RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:alice"),
		map[string]any{"day_of_week": "saturday"},
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_NOT_MEMBER, checkResult.Permissionship)

	// Without caveat context: caveated member
	checkResult, err = RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:alice"),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_CAVEATED_MEMBER, checkResult.Permissionship)
	require.Contains(t, checkResult.MissingCaveatFields, "day_of_week")
}

func TestYAMLDevContextMultipleRelationships(t *testing.T) {
	yamlContent := `schema: |
  definition user {}
  definition group {
    relation member: user
  }
  definition document {
    relation viewer: user | group#member
    permission view = viewer
  }
relationships: |
  group:eng#member@user:alice
  group:eng#member@user:bob
  document:doc1#viewer@group:eng#member
  document:doc1#viewer@user:charlie
`

	result, devErrs, err := NewDevContextForYAML(t.Context(), []byte(yamlContent))
	require.NoError(t, err)
	require.Nil(t, devErrs)
	require.NotNil(t, result)
	defer result.DevContext.Dispose()

	// alice can view via group membership
	checkResult, err := RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:alice"),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.Permissionship)

	// bob can view via group membership
	checkResult, err = RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:bob"),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.Permissionship)

	// charlie can view via direct relation
	checkResult, err = RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:charlie"),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, checkResult.Permissionship)

	// dave cannot view
	checkResult, err = RunCheck(
		result.DevContext,
		tuple.MustParseONR("document:doc1#view"),
		tuple.MustParseSubjectONR("user:dave"),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_NOT_MEMBER, checkResult.Permissionship)
}

func TestNewDevContextForValidationFile(t *testing.T) {
	yaml := `schema: |
  definition user {}
  definition document {
    relation viewer: user
  }
relationships: |
  document:doc1#viewer@nonexistent:foo
`

	// Parse the YAML first, then use the ValidationFile entry point.
	result, devErrs, err := NewDevContextForYAML(t.Context(), []byte(yaml))
	require.NoError(t, err)
	require.NotNil(t, devErrs)
	require.NotEmpty(t, devErrs.InputErrors)
	require.Equal(t, uint32(7), devErrs.InputErrors[0].Line)
	require.Nil(t, result)
}
