package development

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

func TestWarningForPositionNilSourcePosition(t *testing.T) {
	warning := warningForPosition("test-warning", "test message", "source code", nil)
	require.NotNil(t, warning)
	require.Equal(t, "test message", warning.Message) // No warning name appended when nil source position
	require.Equal(t, "source code", warning.SourceCode)
	require.Equal(t, uint32(0), warning.Line)
	require.Equal(t, uint32(0), warning.Column)
}

func TestWarningForPositionWithSourcePosition(t *testing.T) {
	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     4,
		ZeroIndexedColumnPosition: 9,
	}

	warning := warningForPosition("test-warning", "test message", "source code", sourcePos)
	require.NotNil(t, warning)
	require.Equal(t, "test message (test-warning)", warning.Message) // Warning name appended
	require.Equal(t, "source code", warning.SourceCode)
	require.Equal(t, uint32(5), warning.Line)    // 1-indexed
	require.Equal(t, uint32(10), warning.Column) // 1-indexed
}

func TestWarningForMetadata(t *testing.T) {
	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     2,
		ZeroIndexedColumnPosition: 7,
	}

	metadata := &core.Metadata{}

	relation := &core.Relation{
		Name:           "test_relation",
		Metadata:       metadata,
		SourcePosition: sourcePos,
	}

	warning := warningForMetadata("metadata-warning", "metadata message", "source", relation)
	require.NotNil(t, warning)
	require.Equal(t, "metadata message (metadata-warning)", warning.Message)
	require.Equal(t, "source", warning.SourceCode)
	require.Equal(t, uint32(3), warning.Line)   // 1-indexed
	require.Equal(t, uint32(8), warning.Column) // 1-indexed
}

func TestShouldSkipCheckWithoutMetadata(t *testing.T) {
	skip := shouldSkipCheck(nil, "any-check")
	require.False(t, skip)
}

func TestShouldSkipCheckWithMetadataButNoComments(t *testing.T) {
	metadata := &core.Metadata{}
	skip := shouldSkipCheck(metadata, "any-check")
	require.False(t, skip)
}

func TestShouldSkipCheckWithMatchingComment(t *testing.T) {
	// Create a comment message
	comment := &iv1.DocComment{
		Comment: "// spicedb-ignore-warning: test-check",
	}

	commentAny, err := anypb.New(comment)
	require.NoError(t, err)

	metadata := &core.Metadata{
		MetadataMessage: []*anypb.Any{commentAny},
	}

	skip := shouldSkipCheck(metadata, "test-check")
	require.True(t, skip)
}

func TestShouldSkipCheckWithNonMatchingComment(t *testing.T) {
	// Create a comment message
	comment := &iv1.DocComment{
		Comment: "// spicedb-ignore-warning: other-check",
	}

	commentAny, err := anypb.New(comment)
	require.NoError(t, err)

	metadata := &core.Metadata{
		MetadataMessage: []*anypb.Any{commentAny},
	}

	skip := shouldSkipCheck(metadata, "test-check")
	require.False(t, skip)
}

func TestWrappedFunctionedTTU(t *testing.T) {
	fttu := &core.FunctionedTupleToUserset{
		Tupleset: &core.FunctionedTupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
		Function: core.FunctionedTupleToUserset_FUNCTION_ANY,
	}

	wrapped := wrappedFunctionedTTU{fttu}

	// Test GetTupleset
	tupleset := wrapped.GetTupleset()
	require.NotNil(t, tupleset)
	require.Equal(t, "parent", tupleset.GetRelation())

	// Test GetComputedUserset
	computedUserset := wrapped.GetComputedUserset()
	require.NotNil(t, computedUserset)
	require.Equal(t, "member", computedUserset.GetRelation())

	// Test GetArrowString with ANY function
	arrowString, err := wrapped.GetArrowString()
	require.NoError(t, err)
	require.Equal(t, "parent.any(member)", arrowString)
}

func TestWrappedFunctionedTTUWithAllFunction(t *testing.T) {
	fttu := &core.FunctionedTupleToUserset{
		Tupleset: &core.FunctionedTupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
		Function: core.FunctionedTupleToUserset_FUNCTION_ALL,
	}

	wrapped := wrappedFunctionedTTU{fttu}

	// Test GetArrowString with ALL function
	arrowString, err := wrapped.GetArrowString()
	require.NoError(t, err)
	require.Equal(t, "parent.all(member)", arrowString)
}

func TestWrappedTTU(t *testing.T) {
	ttu := &core.TupleToUserset{
		Tupleset: &core.TupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
	}

	wrapped := wrappedTTU{ttu}

	// Test GetTupleset
	tupleset := wrapped.GetTupleset()
	require.NotNil(t, tupleset)
	require.Equal(t, "parent", tupleset.GetRelation())

	// Test GetComputedUserset
	computedUserset := wrapped.GetComputedUserset()
	require.NotNil(t, computedUserset)
	require.Equal(t, "member", computedUserset.GetRelation())

	// Test GetArrowString
	arrowString, err := wrapped.GetArrowString()
	require.NoError(t, err)
	require.Equal(t, "parent->member", arrowString)
}

func TestWarningForPositionWithCastingErrors(t *testing.T) {
	// Test with extremely large line/column numbers to trigger casting errors
	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     4294967296, // Larger than uint32 max (2^32)
		ZeroIndexedColumnPosition: 4294967296, // Larger than uint32 max (2^32)
	}

	warning := warningForPosition("cast-error", "test message", "source code", sourcePos)
	require.NotNil(t, warning)
	require.Equal(t, "test message (cast-error)", warning.Message)
	require.Equal(t, "source code", warning.SourceCode)
	require.Equal(t, uint32(1), warning.Line)   // Should be 1 due to zero fallback + 1
	require.Equal(t, uint32(1), warning.Column) // Should be 1 due to zero fallback + 1
}

func TestWrappedFunctionedTTUUnknownFunction(t *testing.T) {
	fttu := &core.FunctionedTupleToUserset{
		Tupleset: &core.FunctionedTupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
		Function: core.FunctionedTupleToUserset_Function(999), // Unknown function type
	}

	wrapped := wrappedFunctionedTTU{fttu}

	// Test GetArrowString with unknown function - should panic
	require.Panics(t, func() {
		_, _ = wrapped.GetArrowString()
	})
}

func TestCheckExpressionForMixedOperators(t *testing.T) {
	testCases := []struct {
		name          string
		expression    string
		expectWarning bool
		expectedInMsg string
	}{
		{
			name:          "single union operator",
			expression:    "foo + bar",
			expectWarning: false,
		},
		{
			name:          "single intersection operator",
			expression:    "foo & bar",
			expectWarning: false,
		},
		{
			name:          "single exclusion operator",
			expression:    "foo - bar",
			expectWarning: false,
		},
		{
			name:          "multiple same operators - union",
			expression:    "foo + bar + baz",
			expectWarning: false,
		},
		{
			name:          "multiple same operators - intersection",
			expression:    "foo & bar & baz",
			expectWarning: false,
		},
		{
			name:          "multiple same operators - exclusion",
			expression:    "foo - bar - baz",
			expectWarning: false,
		},
		{
			name:          "mixed union and exclusion at same depth",
			expression:    "foo + bar - baz",
			expectWarning: true,
			expectedInMsg: "union (+) and exclusion (-)",
		},
		{
			name:          "mixed exclusion and intersection at same depth",
			expression:    "foo - bar & baz",
			expectWarning: true,
			expectedInMsg: "exclusion (-) and intersection (&)",
		},
		{
			name:          "mixed union and intersection at same depth",
			expression:    "foo + bar & baz",
			expectWarning: true,
			expectedInMsg: "union (+) and intersection (&)",
		},
		{
			name:          "all three operators at same depth",
			expression:    "foo + bar - baz & qux",
			expectWarning: true,
			expectedInMsg: "union (+)",
		},
		{
			name:          "mixed operators with parentheses - different depths",
			expression:    "(foo + bar) - baz",
			expectWarning: false,
		},
		{
			name:          "mixed operators with parentheses - right side",
			expression:    "foo - (bar & baz)",
			expectWarning: false,
		},
		{
			name:          "complex parenthesized expression - no warning",
			expression:    "(foo + bar) - (baz & qux)",
			expectWarning: false,
		},
		{
			name:          "arrow expression should not trigger warning",
			expression:    "parent->view + editor",
			expectWarning: false,
		},
		{
			name:          "arrow expression with mixed operators should trigger",
			expression:    "parent->view + editor - blocked",
			expectWarning: true,
			expectedInMsg: "union (+) and exclusion (-)",
		},
		{
			name:          "function call expression",
			expression:    "parent.all(view) + editor",
			expectWarning: false,
		},
		{
			name:          "empty expression",
			expression:    "",
			expectWarning: false,
		},
		{
			name:          "only identifier",
			expression:    "viewer",
			expectWarning: false,
		},
		{
			name:          "nested parentheses all same operator",
			expression:    "(foo + bar) + (baz + qux)",
			expectWarning: false,
		},
		{
			name:          "deeply nested mixed operators at different depths",
			expression:    "((foo + bar) - baz) & qux",
			expectWarning: false,
		},
		{
			name:          "mixed operators inside parentheses should warn",
			expression:    "(foo + bar - baz)",
			expectWarning: true,
			expectedInMsg: "union (+) and exclusion (-)",
		},
		{
			name:          "mixed operators in nested parens should warn",
			expression:    "qux & (foo + bar - baz)",
			expectWarning: true,
			expectedInMsg: "union (+) and exclusion (-)",
		},
	}

	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     0,
		ZeroIndexedColumnPosition: 0,
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			warning := CheckExpressionForMixedOperators("test_perm", tc.expression, sourcePos)
			if tc.expectWarning {
				require.NotNil(t, warning, "expected warning but got nil")
				require.Contains(t, warning.Message, tc.expectedInMsg, "warning message should contain expected text")
				require.Contains(t, warning.Message, "mixed-operators-without-parentheses")
			} else {
				require.Nil(t, warning, "expected no warning but got: %v", warning)
			}
		})
	}
}

func TestCheckExpressionForMixedOperatorsNilSourcePosition(t *testing.T) {
	warning := CheckExpressionForMixedOperators("test_perm", "foo + bar - baz", nil)
	require.NotNil(t, warning, "should still produce warning with nil source position")
	require.Equal(t, uint32(0), warning.Line)
	require.Equal(t, uint32(0), warning.Column)
}

func TestExtractPermissionExpression(t *testing.T) {
	testCases := []struct {
		name               string
		schema             string
		permissionName     string
		lineNumber         uint64
		expectedExpression string
	}{
		{
			name: "simple permission",
			schema: `definition test {
				permission view = foo + bar
			}`,
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "foo + bar",
		},
		{
			name: "permission with comment after",
			schema: `definition test {
				permission view = foo + bar // some comment
			}`,
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "foo + bar",
		},
		{
			name:               "empty schema",
			schema:             "",
			permissionName:     "view",
			lineNumber:         0,
			expectedExpression: "",
		},
		{
			name:               "multi-line expression with operator at end of line",
			schema:             "definition test {\n\tpermission view = foo +\n\t\tbar - baz\n}",
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "foo + \t\tbar - baz",
		},
		{
			name:               "multi-line expression with parentheses",
			schema:             "definition test {\n\tpermission view = (foo +\n\t\tbar) - baz\n}",
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "(foo + \t\tbar) - baz",
		},
		{
			name: "expression with block comment",
			schema: `definition test {
				permission view = foo /* this is a comment */ + bar
			}`,
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "foo  + bar",
		},
		{
			name: "expression with block comment spanning content",
			schema: `definition test {
				permission view = foo + /* comment */ bar - baz
			}`,
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "foo +  bar - baz",
		},
		{
			name: "multi-line expression without operator at end - single line",
			schema: `definition test {
				permission view = foo + bar
				permission edit = baz
			}`,
			permissionName:     "view",
			lineNumber:         1,
			expectedExpression: "foo + bar",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sourcePos := &core.SourcePosition{
				ZeroIndexedLineNumber:     tc.lineNumber,
				ZeroIndexedColumnPosition: 0,
			}
			schemaLines := strings.Split(tc.schema, "\n")
			result := extractPermissionExpression(schemaLines, tc.permissionName, sourcePos)
			require.Equal(t, tc.expectedExpression, result)
		})
	}
}

func TestExtractPermissionExpressionNilSourcePosition(t *testing.T) {
	schemaLines := strings.Split("definition test { permission view = foo }", "\n")
	result := extractPermissionExpression(schemaLines, "view", nil)
	require.Empty(t, result)
}
