package tables

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/require"
)

func TestParseSelectStatement(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name                string
		query               string
		expectedError       string
		expectedFields      map[string]valueOrRef
		expectedUnsupported bool
	}{
		{
			name:          "not a select",
			query:         "INSERT INTO foo (bar) VALUES (1)",
			expectedError: "only SELECT is supported",
		},
		{
			name:          "multiple from clauses",
			query:         "SELECT * FROM foo, bar",
			expectedError: "a select statement must have exactly one FROM clause",
		},
		{
			name:          "no from clause",
			query:         "SELECT *",
			expectedError: "a select statement must have exactly one FROM clause",
		},
		{
			name:          "unsupported table",
			query:         "SELECT * FROM foo",
			expectedError: "table foo does not exist",
		},
		{
			name:                "unsupported order by",
			query:               "SELECT * FROM relationships ORDER BY name",
			expectedError:       "ORDER BY not supported",
			expectedUnsupported: true,
		},
		{
			name:           "basic select query",
			query:          "SELECT * FROM relationships WHERE resource_id='bar' AND resource_type='qux'",
			expectedFields: map[string]valueOrRef{"resource_id": {value: "bar"}, "resource_type": {value: "qux"}},
		},
		{
			name:                "basic select query with reused field",
			query:               "SELECT * FROM relationships WHERE foo='bar' AND foo='qux'",
			expectedError:       "field foo already set",
			expectedUnsupported: true,
		},
		{
			name:                "basic select query with OR",
			query:               "SELECT * FROM relationships WHERE foo='bar' OR bar='qux'",
			expectedError:       "only AND supported",
			expectedUnsupported: true,
		},
		{
			name:                "basic select with nonequal",
			query:               "SELECT * FROM relationships WHERE foo != 'bar'",
			expectedError:       "only = supported",
			expectedUnsupported: true,
		},
		{
			name:           "basic select with parameter",
			query:          "SELECT * FROM relationships WHERE resource_id = $1",
			expectedFields: map[string]valueOrRef{"resource_id": {parameterIndex: 1}},
		},
		{
			name:           "basic select with parameter inverted",
			query:          "SELECT * FROM relationships WHERE $1 = resource_id",
			expectedFields: map[string]valueOrRef{"resource_id": {parameterIndex: 1}},
		},
		{
			name:                "basic select with field = field",
			query:               "SELECT * FROM relationships WHERE bar = foo",
			expectedUnsupported: true,
			expectedError:       "only one side of the expression can be a column",
		},
		{
			name:                "missing column name",
			query:               "SELECT * FROM relationships WHERE 'foo' = 'bar'",
			expectedUnsupported: true,
			expectedError:       "missing column name",
		},
		{
			name:  "real SELECT",
			query: `SELECT resource_type, resource_id, relation, subject_type, subject_id, optional_subject_relation FROM public.relationships WHERE ((resource_type = 'document')) AND ((relation = 'view')) AND ((subject_type = 'user')) AND ((subject_id = 'tom'))`,
			expectedFields: map[string]valueOrRef{
				"resource_type": {value: "document"},
				"relation":      {value: "view"},
				"subject_type":  {value: "user"},
				"subject_id":    {value: "tom"},
			},
		},
		{
			name:  "SELECT with null for explain",
			query: `SELECT * FROM relationships WHERE ((((SELECT null::text)::text) = resource_id))`,
			expectedFields: map[string]valueOrRef{
				"resource_id": {isSubQueryPlaceholder: true},
			},
		},
		{
			name:                "DISTINCT not supported",
			query:               "SELECT DISTINCT * FROM relationships",
			expectedError:       "DISTINCT not supported",
			expectedUnsupported: true,
		},
		{
			name:                "GROUP BY not supported",
			query:               "SELECT * FROM relationships GROUP BY resource_type",
			expectedError:       "GROUP BY not supported",
			expectedUnsupported: true,
		},
		{
			name:                "HAVING not supported",
			query:               "SELECT * FROM relationships HAVING count(*) > 1",
			expectedError:       "HAVING not supported",
			expectedUnsupported: true,
		},
		{
			name:                "INTO not supported",
			query:               "SELECT * INTO new_table FROM relationships",
			expectedError:       "INTO not supported",
			expectedUnsupported: true,
		},
		{
			name:                "LOCK not supported",
			query:               "SELECT * FROM relationships FOR UPDATE",
			expectedError:       "LOCK not supported",
			expectedUnsupported: true,
		},
		{
			name:                "LIMIT not supported",
			query:               "SELECT * FROM relationships LIMIT 10",
			expectedError:       "LIMIT not supported",
			expectedUnsupported: true,
		},
		{
			name:                "invalid column in SELECT",
			query:               "SELECT nonexistent_column FROM relationships",
			expectedError:       "column nonexistent_column does not exist in table relationships",
			expectedUnsupported: true,
		},
		{
			name:                "invalid field in WHERE",
			query:               "SELECT * FROM relationships WHERE nonexistent_field = 'value'",
			expectedError:       "field nonexistent_field does not exist in table relationships",
			expectedUnsupported: true,
		},
		{
			name:           "basic select with specific columns",
			query:          "SELECT resource_type, resource_id FROM relationships WHERE resource_type = 'document'",
			expectedFields: map[string]valueOrRef{"resource_type": {value: "document"}},
		},
		{
			name:                "unsupported expression - LIKE",
			query:               "SELECT * FROM relationships WHERE resource_type LIKE 'doc%'",
			expectedError:       "only = supported",
			expectedUnsupported: true,
		},
		{
			name:                "unsupported expression - IN",
			query:               "SELECT * FROM relationships WHERE resource_type IN ('doc', 'page')",
			expectedError:       "unsupported expression type",
			expectedUnsupported: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := pg_query.Parse(tc.query)
			require.NoError(t, err)

			slc, err := ParseSelectStatement(parsed.Stmts[0].Stmt.GetSelectStmt(), false)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)

				if tc.expectedUnsupported {
					require.Equal(t, tc.expectedUnsupported, slc.isUnsupported)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedFields, slc.fields)
		})
	}
}
