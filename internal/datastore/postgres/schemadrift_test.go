//go:build datastore

package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/pkg/datastore/test"
)

func init() {
	test.RegisterSchemaSnapshotter(Engine, snapshotPostgresSchema)
}

// postgresSchemaQueries read the schema of the current schema of the connected
// database out of the system catalogs. PostgreSQL has no `SHOW CREATE TABLE`,
// so the pieces a stray DDL statement could change are read individually:
// relations and their columns, indexes, constraints, and per-relation storage
// parameters. Each query returns one text column; the rows are sorted and
// joined into the snapshot.
var postgresSchemaQueries = []string{
	`SELECT 'column ' || c.relkind::text || ' ' || c.relname || '.' || a.attnum::text || ' ' || a.attname ||
	        ' type=' || format_type(a.atttypid, a.atttypmod) ||
	        ' notnull=' || a.attnotnull::text ||
	        ' default=' || coalesce(pg_get_expr(d.adbin, d.adrelid), '<none>') ||
	        ' identity=' || a.attidentity::text || ' generated=' || a.attgenerated::text
	 FROM pg_class c
	 JOIN pg_namespace n ON n.oid = c.relnamespace
	 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
	 LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
	 WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'f')`,

	`SELECT 'index ' || indexname || ': ' || indexdef
	 FROM pg_indexes WHERE schemaname = current_schema()`,

	`SELECT 'constraint ' || conrelid::regclass::text || '.' || conname || ': ' || pg_get_constraintdef(oid)
	 FROM pg_constraint WHERE connamespace = current_schema()::regnamespace`,

	`SELECT 'reloptions ' || c.relkind::text || ' ' || c.relname || ': ' || coalesce(array_to_string(c.reloptions, ','), '<none>')
	 FROM pg_class c
	 JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'm', 'i')`,
}

// snapshotPostgresSchema captures the schema of a PostgreSQL database.
func snapshotPostgresSchema(ctx context.Context, _ testing.TB, uri string) (string, error) {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return "", fmt.Errorf("failed to connect to the postgres test database: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var lines []string
	for _, query := range postgresSchemaQueries {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return "", fmt.Errorf("failed to read the postgres schema: %w", err)
		}

		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				return "", fmt.Errorf("failed to scan a schema row: %w", err)
			}
			lines = append(lines, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("failed to read the postgres schema: %w", err)
		}
	}

	slices.Sort(lines)
	return strings.Join(lines, "\n"), nil
}
