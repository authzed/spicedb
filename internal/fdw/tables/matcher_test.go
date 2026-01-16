package tables

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/require"
)

func TestPatternMatcher(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		query          string
		expectedFields map[string]valueOrRef
		expectedError  string
	}{
		{
			name:  "simple equality",
			query: "SELECT * FROM relationships WHERE resource_type = 'document'",
			expectedFields: map[string]valueOrRef{
				"resource_type": {value: "document"},
			},
		},
		{
			name:  "multiple conditions with AND",
			query: "SELECT * FROM relationships WHERE resource_type = 'document' AND resource_id = 'doc1'",
			expectedFields: map[string]valueOrRef{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
			},
		},
		{
			name:  "parameter reference",
			query: "SELECT * FROM relationships WHERE resource_type = $1",
			expectedFields: map[string]valueOrRef{
				"resource_type": {parameterIndex: 1},
			},
		},
		{
			name:  "inverted equality",
			query: "SELECT * FROM relationships WHERE $1 = resource_type",
			expectedFields: map[string]valueOrRef{
				"resource_type": {parameterIndex: 1},
			},
		},
		{
			name:          "unsupported OR",
			query:         "SELECT * FROM relationships WHERE resource_type = 'document' OR resource_id = 'doc1'",
			expectedError: "only AND supported",
		},
		{
			name:          "unsupported LIKE",
			query:         "SELECT * FROM relationships WHERE resource_type LIKE 'doc%'",
			expectedError: "only = supported",
		},
		{
			name:           "empty WHERE",
			query:          "SELECT * FROM relationships",
			expectedFields: map[string]valueOrRef{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pg_query.Parse(tc.query)
			require.NoError(t, err)

			selectStmt := parsed.Stmts[0].Stmt.GetSelectStmt()
			require.NotNil(t, selectStmt)

			pm := NewPatternMatcher()
			fields, err := pm.MatchWhereClause(selectStmt.GetWhereClause())

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedFields, fields)
		})
	}
}

func TestEqualityPattern(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		query              string
		expectedFieldName  string
		expectedFieldValue valueOrRef
		shouldMatch        bool
	}{
		{
			name:               "column = literal",
			query:              "SELECT * FROM relationships WHERE resource_type = 'document'",
			expectedFieldName:  "resource_type",
			expectedFieldValue: valueOrRef{value: "document"},
			shouldMatch:        true,
		},
		{
			name:               "literal = column",
			query:              "SELECT * FROM relationships WHERE 'document' = resource_type",
			expectedFieldName:  "resource_type",
			expectedFieldValue: valueOrRef{value: "document"},
			shouldMatch:        true,
		},
		{
			name:               "column = parameter",
			query:              "SELECT * FROM relationships WHERE resource_type = $1",
			expectedFieldName:  "resource_type",
			expectedFieldValue: valueOrRef{parameterIndex: 1},
			shouldMatch:        true,
		},
		{
			name:        "column != literal (not equals)",
			query:       "SELECT * FROM relationships WHERE resource_type != 'document'",
			shouldMatch: false,
		},
		{
			name:        "column LIKE pattern",
			query:       "SELECT * FROM relationships WHERE resource_type LIKE 'doc%'",
			shouldMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pg_query.Parse(tc.query)
			require.NoError(t, err)

			selectStmt := parsed.Stmts[0].Stmt.GetSelectStmt()
			require.NotNil(t, selectStmt)

			whereClause := selectStmt.GetWhereClause()
			require.NotNil(t, whereClause)

			pattern := equalityPattern{}
			result, ok := pattern.Match(whereClause)

			if !tc.shouldMatch {
				require.False(t, ok)
				return
			}

			require.True(t, ok)
			require.Equal(t, tc.expectedFieldName, result.Values["field_name"])
			require.Equal(t, tc.expectedFieldValue, result.Values["field_value"])
		})
	}
}

func TestSubQueryPlaceholderPattern(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		query       string
		shouldMatch bool
	}{
		{
			name:        "subquery placeholder",
			query:       "SELECT * FROM relationships WHERE ((SELECT null::text)::text) = resource_id",
			shouldMatch: true,
		},
		{
			name:        "regular literal",
			query:       "SELECT * FROM relationships WHERE resource_id = 'doc1'",
			shouldMatch: false,
		},
		{
			name:        "parameter",
			query:       "SELECT * FROM relationships WHERE resource_id = $1",
			shouldMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pg_query.Parse(tc.query)
			require.NoError(t, err)

			selectStmt := parsed.Stmts[0].Stmt.GetSelectStmt()
			require.NotNil(t, selectStmt)

			whereClause := selectStmt.GetWhereClause()
			require.NotNil(t, whereClause)

			// Get the left expression of the equality
			aExpr := whereClause.GetAExpr()
			require.NotNil(t, aExpr)

			pattern := subQueryPlaceholderPattern{}
			result, ok := pattern.Match(aExpr.Lexpr)

			require.Equal(t, tc.shouldMatch, ok)
			if ok {
				require.True(t, result.Values["is_placeholder"].(bool))
			}
		})
	}
}

func TestColumnRefPattern(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		query           string
		captureName     string
		expectedCapture string
		shouldMatch     bool
	}{
		{
			name:            "simple column reference",
			query:           "SELECT * FROM relationships WHERE resource_type = 'document'",
			captureName:     "col",
			expectedCapture: "resource_type",
			shouldMatch:     true,
		},
		{
			name:        "not a column reference",
			query:       "SELECT * FROM relationships WHERE 'literal' = 'value'",
			captureName: "col",
			shouldMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pg_query.Parse(tc.query)
			require.NoError(t, err)

			selectStmt := parsed.Stmts[0].Stmt.GetSelectStmt()
			require.NotNil(t, selectStmt)

			whereClause := selectStmt.GetWhereClause()
			require.NotNil(t, whereClause)

			aExpr := whereClause.GetAExpr()
			require.NotNil(t, aExpr)

			pattern := columnRefPattern{CaptureName: tc.captureName}
			result, ok := pattern.Match(aExpr.Lexpr)

			require.Equal(t, tc.shouldMatch, ok)
			if ok && tc.captureName != "" {
				require.Equal(t, tc.expectedCapture, result.Values[tc.captureName])
			}
		})
	}
}

// BenchmarkPatternMatcher compares the performance of pattern matching vs old approach
func BenchmarkPatternMatcher(b *testing.B) {
	query := "SELECT * FROM relationships WHERE resource_type = 'document' AND resource_id = 'doc1' AND relation = 'viewer'"
	parsed, err := pg_query.Parse(query)
	require.NoError(b, err)

	selectStmt := parsed.Stmts[0].Stmt.GetSelectStmt()
	require.NotNil(b, selectStmt)

	whereClause := selectStmt.GetWhereClause()

	pm := NewPatternMatcher()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pm.MatchWhereClause(whereClause)
	}
}
