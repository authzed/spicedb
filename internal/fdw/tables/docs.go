// Package tables implements SQL table handlers for SpiceDB entities.
//
// This package provides query handlers for virtual SQL tables that map to
// SpiceDB concepts:
//   - permissions: Check permissions and lookup resources/subjects
//   - relationships: Read, write, and delete relationships
//   - schema: Read SpiceDB schema definitions
//
// Each table handler is responsible for parsing SELECT/INSERT/DELETE statements,
// converting them to appropriate SpiceDB API calls, and formatting results in
// Postgres wire protocol format.
package tables
