// Package stats provides statistics tracking for FDW query operations.
//
// This package collects query execution statistics including cost metrics,
// row counts, and result widths using quantile streams. The statistics are
// used to generate accurate EXPLAIN plans and help the Postgres query planner
// make better decisions.
package stats
