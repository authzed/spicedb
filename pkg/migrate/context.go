package migrate

// MigrationVariable contains constants that can be used as context keys that might
// be relevant in a number of different migration scenarios.
type MigrationVariable int

const (
	// BackfillBatchSize represents the number of items that should be backfilled in a
	// single step of an incremental backfill, and should be of type uint64.
	BackfillBatchSize MigrationVariable = iota
)
