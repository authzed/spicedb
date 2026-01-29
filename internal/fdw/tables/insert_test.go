package tables

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pgquery "github.com/wasilibs/go-pgquery"
)

func TestParseInsertStatement(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		query         string
		expectedError string
		expectedCols  []string
	}{
		{
			name:          "not an insert",
			query:         "SELECT * FROM relationships",
			expectedError: "query must be an INSERT statement",
		},
		{
			name:          "unsupported table",
			query:         "INSERT INTO foo (bar) VALUES (1)",
			expectedError: "table foo does not exist",
		},
		{
			name:          "table without insert support",
			query:         "INSERT INTO permissions (resource_type) VALUES ('document')",
			expectedError: "table permissions does not support INSERT",
		},
		{
			name:          "missing required columns",
			query:         "INSERT INTO relationships (resource_type) VALUES ('document')",
			expectedError: "expected at least",
		},
		{
			name:          "too few columns",
			query:         "INSERT INTO relationships (resource_type, resource_id) VALUES ('document', 'doc1')",
			expectedError: "expected at least",
		},
		{
			name:          "invalid column name",
			query:         "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, invalid_col) VALUES ('document', 'doc1', 'viewer', 'user', 'alice', 'value')",
			expectedError: "column invalid_col does not exist",
		},
		{
			name:  "basic insert with required columns",
			query: "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ('document', 'doc1', 'viewer', 'user', 'alice', '')",
			expectedCols: []string{
				"resource_type",
				"resource_id",
				"relation",
				"subject_type",
				"subject_id",
				"optional_subject_relation",
			},
		},
		{
			name:  "insert with all columns",
			query: "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation, caveat_name, caveat_context) VALUES ('document', 'doc1', 'viewer', 'user', 'alice', '', '', '')",
			expectedCols: []string{
				"resource_type",
				"resource_id",
				"relation",
				"subject_type",
				"subject_id",
				"optional_subject_relation",
				"caveat_name",
				"caveat_context",
			},
		},
		{
			name:  "insert with parameters",
			query: "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ($1, $2, $3, $4, $5, $6)",
			expectedCols: []string{
				"resource_type",
				"resource_id",
				"relation",
				"subject_type",
				"subject_id",
				"optional_subject_relation",
			},
		},
		{
			name:  "insert into schema table",
			query: "INSERT INTO schema (schema_text) VALUES ('definition user {}')",
			expectedCols: []string{
				"schema_text",
			},
		},
		{
			name:          "insert with OVERRIDING",
			query:         "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) OVERRIDING SYSTEM VALUE VALUES ('document', 'doc1', 'viewer', 'user', 'alice', '')",
			expectedError: "INSERT ... OVERRIDING is not supported",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pgquery.Parse(tc.query)
			require.NoError(t, err)

			ctx := context.Background()
			stmt, err := ParseInsertStatement(ctx, parsed.Stmts[0].Stmt.GetInsertStmt())

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmt)
			require.Equal(t, tc.expectedCols, stmt.colNames)
		})
	}
}

func TestInsertStatementReturningColumns(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		query                 string
		expectedReturningCols int
	}{
		{
			name:                  "insert without returning",
			query:                 "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ('document', 'doc1', 'viewer', 'user', 'alice', '')",
			expectedReturningCols: 0,
		},
		{
			name:                  "insert with returning",
			query:                 "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ('document', 'doc1', 'viewer', 'user', 'alice', '') RETURNING consistency",
			expectedReturningCols: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pgquery.Parse(tc.query)
			require.NoError(t, err)

			ctx := context.Background()
			stmt, err := ParseInsertStatement(ctx, parsed.Stmts[0].Stmt.GetInsertStmt())
			require.NoError(t, err)
			require.NotNil(t, stmt)

			cols := stmt.ReturningColumns()
			require.Len(t, cols, tc.expectedReturningCols)
		})
	}
}
