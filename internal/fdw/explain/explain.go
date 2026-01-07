package explain

import "fmt"

// Explain represents a Postgres EXPLAIN query plan with cost and row estimates.
type Explain struct {
	Operation                    string
	TableName                    string
	StartupCost                  float32
	TotalCost                    float32
	EstimatedRows                int32
	EstimatedAverageWidthInBytes int32
}

// String formats the explain plan in Postgres EXPLAIN output format.
func (e Explain) String() string {
	return fmt.Sprintf("%s on %s (cost=%.2f..%.2f rows=%d width=%d)",
		e.Operation,
		e.TableName,
		e.StartupCost,
		e.TotalCost,
		e.EstimatedRows,
		e.EstimatedAverageWidthInBytes,
	)
}

const expensive = 500_000

// Default returns a default explain plan with conservative cost estimates.
func Default(operation, tableName string) Explain {
	return Explain{operation, tableName, 10, 10, 1000, 100}
}

// Unsupported is a pre-formatted EXPLAIN plan with extremely high cost.
// This is used to discourage Postgres from selecting unsupported query plans.
var Unsupported = Explain{"Unsupported", "irrelevant", expensive, expensive, expensive, expensive}.String()
