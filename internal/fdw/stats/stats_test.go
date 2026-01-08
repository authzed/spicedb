package stats

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/fdw/explain"
)

func TestNewPackage(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		table     string
		operation string
	}{
		{
			name:      "relationships table",
			table:     "relationships",
			operation: "Foreign Scan",
		},
		{
			name:      "permissions table",
			table:     "permissions",
			operation: "Foreign Scan",
		},
		{
			name:      "schema table",
			table:     "schema",
			operation: "Seq Scan",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg := NewPackage(tc.table, tc.operation)
			require.NotNil(t, pkg)
			require.Equal(t, tc.table, pkg.table)
			require.Equal(t, tc.operation, pkg.operation)
			require.NotNil(t, pkg.cost)
			require.NotNil(t, pkg.rows)
			require.NotNil(t, pkg.width)
		})
	}
}

func TestAddWidthSample(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		samples []uint
	}{
		{
			name:    "single sample",
			samples: []uint{100},
		},
		{
			name:    "multiple samples",
			samples: []uint{50, 100, 150, 200},
		},
		{
			name:    "varying widths",
			samples: []uint{10, 500, 250, 100, 300},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg := NewPackage("test_table", "Test Scan")
			for _, sample := range tc.samples {
				pkg.AddWidthSample(sample)
			}

			// Verify the package still works after adding samples
			explainStr := pkg.Explain()
			require.NotEmpty(t, explainStr)
			require.Contains(t, explainStr, "test_table")
		})
	}
}

func TestAddSample(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		samples []struct {
			cost float32
			rows uint
		}
	}{
		{
			name: "single sample",
			samples: []struct {
				cost float32
				rows uint
			}{
				{cost: 100.0, rows: 1000},
			},
		},
		{
			name: "multiple samples",
			samples: []struct {
				cost float32
				rows uint
			}{
				{cost: 50.0, rows: 500},
				{cost: 100.0, rows: 1000},
				{cost: 150.0, rows: 1500},
			},
		},
		{
			name: "varying costs and rows",
			samples: []struct {
				cost float32
				rows uint
			}{
				{cost: 10.0, rows: 100},
				{cost: 500.0, rows: 5000},
				{cost: 250.0, rows: 2500},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg := NewPackage("test_table", "Test Scan")
			for _, sample := range tc.samples {
				pkg.AddSample(sample.cost, sample.rows)
			}

			// Verify the package still works after adding samples
			explainStr := pkg.Explain()
			require.NotEmpty(t, explainStr)
			require.Contains(t, explainStr, "test_table")
		})
	}
}

func TestExplainWithNoSamples(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		table     string
		operation string
	}{
		{
			name:      "relationships",
			table:     "relationships",
			operation: "Foreign Scan",
		},
		{
			name:      "permissions",
			table:     "permissions",
			operation: "Foreign Scan",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg := NewPackage(tc.table, tc.operation)
			result := pkg.Explain()

			// When no samples, should return default explain
			expected := explain.Default(tc.operation, tc.table).String()
			require.Equal(t, expected, result)
		})
	}
}

func TestExplainWithSamples(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		table     string
		operation string
		samples   []struct {
			cost  float32
			rows  uint
			width uint
		}
		expectedCost   float32
		expectedRows   int32
		expectedWidth  int32
		costTolerance  float32
		rowsTolerance  int32
		widthTolerance int32
	}{
		{
			name:      "single sample set",
			table:     "relationships",
			operation: "Foreign Scan",
			samples: []struct {
				cost  float32
				rows  uint
				width uint
			}{
				{cost: 100.0, rows: 1000, width: 50},
			},
			expectedCost:   100.0,
			expectedRows:   1000,
			expectedWidth:  50,
			costTolerance:  1.0,
			rowsTolerance:  10,
			widthTolerance: 5,
		},
		{
			name:      "multiple sample sets - median",
			table:     "permissions",
			operation: "Foreign Scan",
			samples: []struct {
				cost  float32
				rows  uint
				width uint
			}{
				{cost: 50.0, rows: 500, width: 40},
				{cost: 100.0, rows: 1000, width: 50},
				{cost: 150.0, rows: 1500, width: 60},
			},
			// Quantile streams use approximate algorithms, so median might be one of the samples
			// For 3 samples, expect something in the middle range
			expectedCost:   100.0,
			expectedRows:   1000,
			expectedWidth:  50,
			costTolerance:  60.0, // Allow for approximate quantile calculation
			rowsTolerance:  600,  // Allow for approximate quantile calculation
			widthTolerance: 15,   // Allow for approximate quantile calculation
		},
		{
			name:      "many samples - median calculation",
			table:     "schema",
			operation: "Seq Scan",
			samples: []struct {
				cost  float32
				rows  uint
				width uint
			}{
				{cost: 10.0, rows: 100, width: 20},
				{cost: 20.0, rows: 200, width: 30},
				{cost: 30.0, rows: 300, width: 40},
				{cost: 40.0, rows: 400, width: 50},
				{cost: 50.0, rows: 500, width: 60},
			},
			// Median of 5 values should be around the 3rd value (30, 300, 40)
			expectedCost:   30.0,
			expectedRows:   300,
			expectedWidth:  40,
			costTolerance:  10.0,
			rowsTolerance:  100,
			widthTolerance: 10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pkg := NewPackage(tc.table, tc.operation)

			for _, sample := range tc.samples {
				pkg.AddSample(sample.cost, sample.rows)
				pkg.AddWidthSample(sample.width)
			}

			result := pkg.Explain()
			require.NotEmpty(t, result)
			require.Contains(t, result, tc.operation)
			require.Contains(t, result, tc.table)
			require.Contains(t, result, "cost=")
			require.Contains(t, result, "rows=")
			require.Contains(t, result, "width=")

			// Parse the explain output to verify actual values
			// Expected format: "Foreign Scan on relationships (cost=100.00..100.00 rows=1000 width=50)"
			var operation, tableName string
			var startupCost, totalCost float32
			var rows, width int32

			_, err := fmt.Sscanf(result,
				"%s Scan on %s (cost=%f..%f rows=%d width=%d)",
				&operation, &tableName, &startupCost, &totalCost, &rows, &width)
			require.NoError(t, err, "Failed to parse explain output: %s", result)

			// Verify values are within tolerance of expected median
			require.InDelta(t, tc.expectedCost, startupCost, float64(tc.costTolerance),
				"Startup cost %f not within tolerance %f of expected %f", startupCost, tc.costTolerance, tc.expectedCost)
			require.InDelta(t, tc.expectedCost, totalCost, float64(tc.costTolerance),
				"Total cost %f not within tolerance %f of expected %f", totalCost, tc.costTolerance, tc.expectedCost)
			require.InDelta(t, tc.expectedRows, rows, float64(tc.rowsTolerance),
				"Rows %d not within tolerance %d of expected %d", rows, tc.rowsTolerance, tc.expectedRows)
			require.InDelta(t, tc.expectedWidth, width, float64(tc.widthTolerance),
				"Width %d not within tolerance %d of expected %d", width, tc.widthTolerance, tc.expectedWidth)
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	pkg := NewPackage("test_table", "Test Scan")

	// Simulate concurrent access from multiple goroutines
	done := make(chan bool, 3)

	go func() {
		for i := 0; i < 100; i++ {
			pkg.AddSample(float32(i), uint(i*10))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			pkg.AddWidthSample(uint(i * 5))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_ = pkg.Explain()
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	<-done
	<-done
	<-done

	// Verify the package is still in a valid state
	result := pkg.Explain()
	require.NotEmpty(t, result)
	require.Contains(t, result, "test_table")
}

func TestExplainFormat(t *testing.T) {
	t.Parallel()

	pkg := NewPackage("relationships", "Foreign Scan")
	pkg.AddSample(123.45, 5000)
	pkg.AddWidthSample(75)

	result := pkg.Explain()

	// Verify the format matches Postgres EXPLAIN output
	require.Contains(t, result, "Foreign Scan on relationships")
	require.True(t, strings.Contains(result, "cost=") && strings.Contains(result, ".."))
	require.Contains(t, result, "rows=")
	require.Contains(t, result, "width=")

	// Verify numbers are present (even if we don't know exact median values)
	parts := strings.Split(result, " ")
	require.Greater(t, len(parts), 3, "Explain output should have multiple parts")
}
