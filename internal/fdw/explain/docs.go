// Package explain provides EXPLAIN query plan generation for the FDW.
//
// This package formats query execution plans in Postgres-compatible format,
// including cost estimates and row count predictions. It supports generating
// both default plans and plans marked as unsupported to guide Postgres query
// planner decisions.
package explain
