package tables

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pgquery "github.com/wasilibs/go-pgquery"
)

func TestParseDeleteStatement(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                string
		query               string
		expectedError       string
		expectedFields      map[string]valueOrRef
		expectedUnsupported bool
	}{
		{
			name:          "not a delete",
			query:         "SELECT * FROM relationships",
			expectedError: "query must be an DELETE statement",
		},
		{
			name:          "unsupported table",
			query:         "DELETE FROM foo WHERE bar = 'baz'",
			expectedError: "table foo does not exist",
		},
		{
			name:          "table without delete support",
			query:         "DELETE FROM permissions WHERE resource_type = 'document'",
			expectedError: "table permissions does not support DELETE",
		},
		{
			name:          "delete with USING clause",
			query:         "DELETE FROM relationships USING other_table WHERE relationships.resource_type = other_table.type",
			expectedError: "DELETE statements with USING are not supported",
		},
		{
			name:          "delete with WITH clause",
			query:         "WITH temp AS (SELECT * FROM foo) DELETE FROM relationships WHERE resource_type = 'document'",
			expectedError: "DELETE statements with WITH are not supported",
		},
		{
			name:  "basic delete with where clause",
			query: "DELETE FROM relationships WHERE resource_type = 'document'",
			expectedFields: map[string]valueOrRef{
				"resource_type": {value: "document"},
			},
		},
		{
			name:  "delete with multiple conditions",
			query: "DELETE FROM relationships WHERE resource_type = 'document' AND resource_id = 'doc1' AND relation = 'viewer'",
			expectedFields: map[string]valueOrRef{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
				"relation":      {value: "viewer"},
			},
		},
		{
			name:  "delete with all relationship fields",
			query: "DELETE FROM relationships WHERE resource_type = 'document' AND resource_id = 'doc1' AND relation = 'viewer' AND subject_type = 'user' AND subject_id = 'alice'",
			expectedFields: map[string]valueOrRef{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
				"relation":      {value: "viewer"},
				"subject_type":  {value: "user"},
				"subject_id":    {value: "alice"},
			},
		},
		{
			name:  "delete with parameters",
			query: "DELETE FROM relationships WHERE resource_type = $1 AND resource_id = $2",
			expectedFields: map[string]valueOrRef{
				"resource_type": {parameterIndex: 1},
				"resource_id":   {parameterIndex: 2},
			},
		},
		{
			name:           "delete without where clause",
			query:          "DELETE FROM relationships",
			expectedFields: map[string]valueOrRef{},
		},
		{
			name:                "delete with invalid field",
			query:               "DELETE FROM relationships WHERE invalid_field = 'value'",
			expectedError:       "field invalid_field does not exist",
			expectedUnsupported: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pgquery.Parse(tc.query)
			require.NoError(t, err)

			ctx := context.Background()
			stmt, err := ParseDeleteStatement(ctx, parsed.Stmts[0].Stmt.GetDeleteStmt())

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)

				if tc.expectedUnsupported {
					require.NotNil(t, stmt)
					require.True(t, stmt.isUnsupported)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)
			require.False(t, stmt.isUnsupported)
			require.Equal(t, tc.expectedFields, stmt.fields)
		})
	}
}

func TestDeleteStatementReturningColumns(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		query                 string
		expectedReturningCols int
	}{
		{
			name:                  "delete without returning",
			query:                 "DELETE FROM relationships WHERE resource_type = 'document'",
			expectedReturningCols: 0,
		},
		{
			name:                  "delete with returning",
			query:                 "DELETE FROM relationships WHERE resource_type = 'document' RETURNING consistency",
			expectedReturningCols: 1,
		},
		{
			name:                  "delete with returning multiple columns",
			query:                 "DELETE FROM relationships WHERE resource_type = 'document' RETURNING resource_type, resource_id",
			expectedReturningCols: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pgquery.Parse(tc.query)
			require.NoError(t, err)

			ctx := context.Background()
			stmt, err := ParseDeleteStatement(ctx, parsed.Stmts[0].Stmt.GetDeleteStmt())
			require.NoError(t, err)
			require.NotNil(t, stmt)

			cols := stmt.ReturningColumns()
			require.Len(t, cols, tc.expectedReturningCols)
		})
	}
}
