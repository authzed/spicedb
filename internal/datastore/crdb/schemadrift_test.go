//go:build datastore

package crdb

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
	test.RegisterSchemaSnapshotter(Engine, snapshotCRDBSchema)
}

// snapshotCRDBSchema captures the schema of a CockroachDB database as the
// CREATE statements the server itself reports. They carry the table storage
// parameters — the row-level TTL settings among them — which is where schema
// drift has actually happened.
func snapshotCRDBSchema(ctx context.Context, _ testing.TB, uri string) (string, error) {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return "", fmt.Errorf("failed to connect to the cockroachdb test database: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, "SHOW CREATE ALL TABLES")
	if err != nil {
		return "", fmt.Errorf("failed to read the cockroachdb schema: %w", err)
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var createStatement string
		if err := rows.Scan(&createStatement); err != nil {
			return "", fmt.Errorf("failed to scan a create statement: %w", err)
		}
		statements = append(statements, createStatement)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("failed to read the cockroachdb schema: %w", err)
	}

	// SHOW CREATE ALL TABLES orders by dependency rather than by name, so sort
	// to keep two snapshots of the same schema textually identical.
	slices.Sort(statements)
	return strings.Join(statements, "\n"), nil
}
