package tables

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectStatementAccessors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		stmt         *SelectStatement
		expectedName string
	}{
		{
			name: "relationships table",
			stmt: &SelectStatement{
				tableName: "relationships",
				tableDef:  tables["relationships"],
			},
			expectedName: "relationships",
		},
		{
			name: "permissions table",
			stmt: &SelectStatement{
				tableName: "permissions",
				tableDef:  tables["permissions"],
			},
			expectedName: "permissions",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expectedName, tc.stmt.TableName())
			require.Equal(t, tc.stmt.isUnsupported, tc.stmt.IsUnsupported())
		})
	}
}

func TestFieldMapHasFields(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		fields   fieldMap[string]
		check    []string
		expected bool
	}{
		{
			name: "all fields present",
			fields: fieldMap[string]{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
			},
			check:    []string{"resource_type", "resource_id"},
			expected: true,
		},
		{
			name: "some fields missing",
			fields: fieldMap[string]{
				"resource_type": {value: "document"},
			},
			check:    []string{"resource_type", "resource_id"},
			expected: false,
		},
		{
			name:     "empty map",
			fields:   fieldMap[string]{},
			check:    []string{"resource_type"},
			expected: false,
		},
		{
			name: "checking empty list",
			fields: fieldMap[string]{
				"resource_type": {value: "document"},
			},
			check:    []string{},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := tc.fields.hasFields(tc.check...)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestTableDefinitionMethods(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		tableDef tableDefinition
	}{
		{
			name:     "relationships table",
			tableDef: tables["relationships"],
		},
		{
			name:     "permissions table",
			tableDef: tables["permissions"],
		},
		{
			name:     "schema table",
			tableDef: tables["schema"],
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Test columnNames
			names := tc.tableDef.columnNames()
			require.NotEmpty(t, names)
			require.Len(t, names, len(tc.tableDef.schema))

			// Test hasColumn with valid column
			if len(names) > 0 {
				require.True(t, tc.tableDef.hasColumn(names[0]))
			}

			// Test hasColumn with invalid column
			require.False(t, tc.tableDef.hasColumn("invalid_column_name"))

			// Test getSchemaColumn with valid column
			if len(names) > 0 {
				col, ok := tc.tableDef.getSchemaColumn(names[0])
				require.True(t, ok)
				require.Equal(t, names[0], col.Name)
			}

			// Test getSchemaColumn with invalid column
			_, ok := tc.tableDef.getSchemaColumn("invalid_column_name")
			require.False(t, ok)
		})
	}
}
