// Package fdw implements a Postgres Foreign Data Wrapper (FDW) proxy server for SpiceDB.
//
// This package provides a Postgres-compatible interface that allows querying SpiceDB
// permissions data using standard SQL queries through Postgres Foreign Data Wrapper.
// The proxy accepts Postgres wire protocol connections and translates SQL queries into
// SpiceDB API calls.
//
// The main entry point is PgBackend, which handles the Postgres wire protocol and routes
// queries to the appropriate handlers for permissions, relationships, and schema operations.
//
// Supported Operations:
//   - SELECT queries on permissions and relationships tables
//   - INSERT/DELETE operations on relationships
//   - Transaction management (BEGIN, COMMIT, ABORT)
//   - Cursor-based result iteration (DECLARE, FETCH, CLOSE)
//   - EXPLAIN queries for query planning
//
// Example Usage:
//
//	client := authzed.NewClient(...)
//	backend := fdw.NewPgBackend(client, "postgres", "password")
//	err := backend.Run(ctx, ":5432")
package fdw
