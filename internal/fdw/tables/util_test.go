package tables

import (
	"testing"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/require"
)

func TestStringValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		valueOrRef    valueOrRef
		parameters    []wire.Parameter
		expectedValue string
		expectedError string
	}{
		{
			name:          "direct value",
			valueOrRef:    valueOrRef{value: "test_value"},
			expectedValue: "test_value",
		},
		{
			name:          "parameter index",
			valueOrRef:    valueOrRef{parameterIndex: 1},
			parameters:    []wire.Parameter{mockParameter("param0"), mockParameter("param1")},
			expectedValue: "param1",
		},
		{
			name:          "parameter index out of range",
			valueOrRef:    valueOrRef{parameterIndex: 5},
			parameters:    []wire.Parameter{mockParameter("param0")},
			expectedError: "parameter index out of range",
		},
		{
			name:          "empty value",
			valueOrRef:    valueOrRef{value: ""},
			expectedError: "value is empty",
		},
		{
			name:          "subquery placeholder not supported",
			valueOrRef:    valueOrRef{isSubQueryPlaceholder: true},
			expectedError: "subquery placeholders are not supported in permissions table",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := stringValue(tc.valueOrRef, tc.parameters)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedValue, result)
		})
	}
}

func TestOptionalStringValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		valueOrRef    valueOrRef
		parameters    []wire.Parameter
		expectedValue string
		expectedError string
	}{
		{
			name:          "direct value",
			valueOrRef:    valueOrRef{value: "test_value"},
			expectedValue: "test_value",
		},
		{
			name:          "empty value is allowed",
			valueOrRef:    valueOrRef{value: ""},
			expectedValue: "",
		},
		{
			name:          "parameter index",
			valueOrRef:    valueOrRef{parameterIndex: 1},
			parameters:    []wire.Parameter{mockParameter("param0"), mockParameter("param1")},
			expectedValue: "param1",
		},
		{
			name:          "parameter index out of range",
			valueOrRef:    valueOrRef{parameterIndex: 5},
			parameters:    []wire.Parameter{mockParameter("param0")},
			expectedError: "parameter index out of range",
		},
		{
			name:          "subquery placeholder not supported",
			valueOrRef:    valueOrRef{isSubQueryPlaceholder: true},
			expectedError: "subquery placeholders are not supported in permissions table",
		},
		{
			name:          "zero value",
			valueOrRef:    valueOrRef{},
			expectedValue: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := optionalStringValue(tc.valueOrRef, tc.parameters)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedValue, result)
		})
	}
}

func TestReturningColumnsFromQuery(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		query              string
		expectedError      string
		expectedColumnName string
	}{
		{
			name:               "insert with returning consistency",
			query:              "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ('doc', 'id', 'rel', 'user', 'alice', '') RETURNING consistency",
			expectedColumnName: "consistency",
		},
		{
			name:               "delete with returning consistency",
			query:              "DELETE FROM relationships WHERE resource_type = 'doc' RETURNING consistency",
			expectedColumnName: "consistency",
		},
		{
			name:               "insert without returning",
			query:              "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ('doc', 'id', 'rel', 'user', 'alice', '')",
			expectedColumnName: "",
		},
		{
			name:          "returning invalid column",
			query:         "INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation) VALUES ('doc', 'id', 'rel', 'user', 'alice', '') RETURNING invalid_column",
			expectedError: "returning column \"invalid_column\" does not exist",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pg_query.Parse(tc.query)
			require.NoError(t, err)
			require.NotEmpty(t, parsed.Stmts)

			var returningQuery returningQuery
			if insertStmt := parsed.Stmts[0].Stmt.GetInsertStmt(); insertStmt != nil {
				returningQuery = insertStmt
			} else if deleteStmt := parsed.Stmts[0].Stmt.GetDeleteStmt(); deleteStmt != nil {
				returningQuery = deleteStmt
			} else {
				t.Fatal("Expected INSERT or DELETE statement")
			}

			tableDef := tables["relationships"]
			columns, err := returningColumnsFromQuery(tableDef, returningQuery)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)

			if tc.expectedColumnName == "" {
				require.Empty(t, columns)
			} else {
				require.Len(t, columns, 1)
				require.Equal(t, tc.expectedColumnName, columns[0].Name)
			}
		})
	}
}

// mockParameter creates a wire.Parameter for testing
func mockParameter(value string) wire.Parameter {
	return wire.NewParameter(nil, wire.TextFormat, []byte(value))
}
