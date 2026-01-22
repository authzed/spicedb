package explain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplainString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		explain  Explain
		expected string
	}{
		{
			name: "basic explain",
			explain: Explain{
				Operation:                    "Seq Scan",
				TableName:                    "relationships",
				StartupCost:                  0.00,
				TotalCost:                    100.00,
				EstimatedRows:                1000,
				EstimatedAverageWidthInBytes: 50,
			},
			expected: "Seq Scan on relationships (cost=0.00..100.00 rows=1000 width=50)",
		},
		{
			name: "explain with high cost",
			explain: Explain{
				Operation:                    "Foreign Scan",
				TableName:                    "permissions",
				StartupCost:                  10.00,
				TotalCost:                    50000.00,
				EstimatedRows:                100000,
				EstimatedAverageWidthInBytes: 200,
			},
			expected: "Foreign Scan on permissions (cost=10.00..50000.00 rows=100000 width=200)",
		},
		{
			name: "explain with zero values",
			explain: Explain{
				Operation:                    "Test Scan",
				TableName:                    "test_table",
				StartupCost:                  0,
				TotalCost:                    0,
				EstimatedRows:                0,
				EstimatedAverageWidthInBytes: 0,
			},
			expected: "Test Scan on test_table (cost=0.00..0.00 rows=0 width=0)",
		},
		{
			name: "explain with fractional costs",
			explain: Explain{
				Operation:                    "Index Scan",
				TableName:                    "schema",
				StartupCost:                  1.5,
				TotalCost:                    25.75,
				EstimatedRows:                10,
				EstimatedAverageWidthInBytes: 512,
			},
			expected: "Index Scan on schema (cost=1.50..25.75 rows=10 width=512)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := tc.explain.String()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestDefault(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		operation string
		tableName string
	}{
		{
			name:      "relationships table",
			operation: "Foreign Scan",
			tableName: "relationships",
		},
		{
			name:      "permissions table",
			operation: "Foreign Scan",
			tableName: "permissions",
		},
		{
			name:      "schema table",
			operation: "Seq Scan",
			tableName: "schema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := Default(tc.operation, tc.tableName)
			require.Equal(t, tc.operation, result.Operation)
			require.Equal(t, tc.tableName, result.TableName)
			require.InDelta(t, float32(10), result.StartupCost, 0.01)
			require.InDelta(t, float32(10), result.TotalCost, 0.01)
			require.Equal(t, int32(1000), result.EstimatedRows)
			require.Equal(t, int32(100), result.EstimatedAverageWidthInBytes)

			// Verify it produces a valid string
			str := result.String()
			require.Contains(t, str, tc.operation)
			require.Contains(t, str, tc.tableName)
			require.Contains(t, str, "cost=10.00..10.00")
			require.Contains(t, str, "rows=1000")
			require.Contains(t, str, "width=100")
		})
	}
}

func TestUnsupported(t *testing.T) {
	t.Parallel()

	// Verify Unsupported is a string with very high costs
	require.Contains(t, Unsupported, "Unsupported")
	require.Contains(t, Unsupported, "irrelevant")
	require.Contains(t, Unsupported, "500000")

	// Verify it follows the expected format
	require.Contains(t, Unsupported, "cost=")
	require.Contains(t, Unsupported, "rows=")
	require.Contains(t, Unsupported, "width=")
}

func TestExplainComparison(t *testing.T) {
	t.Parallel()

	// Test that Default produces lower cost than Unsupported constants
	defaultExplain := Default("Foreign Scan", "test")
	require.Less(t, defaultExplain.StartupCost, float32(expensive))
	require.Less(t, defaultExplain.TotalCost, float32(expensive))
	require.Less(t, defaultExplain.EstimatedRows, int32(expensive))
	require.Less(t, defaultExplain.EstimatedAverageWidthInBytes, int32(expensive))
}
